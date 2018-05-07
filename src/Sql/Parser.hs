module Sql.Parser where

import Control.Monad (guard, void, when)
import Data.Char (toLower)
import Data.Maybe (catMaybes)



import Sql.Lexer
import Sql.Syntax

import Text.Parsec 
import Text.Parsec.Expr


parseBinaryOperator :: Parser BinOperator
parseBinaryOperator = (Equality <$> parseEqualOperator) <|> (Arithmetic <$> parseArithOperator)

parseEqualOperator :: Parser EqualityOperator
parseEqualOperator 
  = reservedOp "=" *> pure Equals
  <|> reservedOp "!=" *> pure NotEquals

parseArithOperator :: Parser ArithmeticOperator
parseArithOperator
    = reservedOp "+" *> pure Plus
    <|> reservedOp "-" *> pure Minus
    <|> reservedOp "/" *> pure Divide
    <|> reservedOp "*" *> pure Times
    

parseTerm' :: Parser Term
parseTerm' 
  = StringLit <$> parseStringLiteral <*> parseCoercion
  <|> IntLit <$> integer <*> parseCoercion
  <|> FloatLit <$> float <*> parseCoercion
  <|> reservedOp "*" *> pure Star
  <|> (try parseFunctionApp)
  <|> Iden <$> (optionMaybe $ try $ identifier <* dot) <*> identifier <*> parseCoercion

parseFunction :: Parser Function
parseFunction 
  =   symbol "sum" *> (pure FnSum)
  <|> symbol "avg" *> (pure FnAvg)

parseFunctionApp :: Parser Term
parseFunctionApp = do
  fn <- parseFunction
  args <- parens (commaSep parseValueExpr) 
  coerc <- parseCoercion
  return $ App fn args coerc

parseStringLiteral :: Parser String
parseStringLiteral = do
    _ <- char '\''
    s <- many $ noneOf "'"
    _ <- char '\''
    return s
 
  
parseBinOp :: Parser (Term -> Term -> Term)
parseBinOp = flip BinOp  <$> parseBinaryOperator

table = [ [ ]
        , [Infix parseBinOp AssocLeft ]
        ]

parseValueExpr :: Parser ValueExpr
parseValueExpr = ValueExpr <$> (buildExpressionParser table parseTerm')

parseCoercion :: Parser (Maybe ValueType)
parseCoercion = optionMaybe $ do
  _ <- reserved "as"
  _ <- reserved "type"
  parseValueType


-- TODO
parseSetQuantifier  :: Parser SetQuantifier
parseSetQuantifier = pure SQDefault

parsePath :: Parser String
parsePath = many1 (alphaNum <|> oneOf "/.") <* spaces

parseFrom :: Parser [TableRef]
parseFrom = do
  _ <- reserved "from"
  commaSep tref
  where
    tref  = TablePath <$> parsePath <*> (optionMaybe identifier)
          <|> pDash *> (pure StdIn)
          <|> TableString <$> stringLiteral
    pDash = reservedOp "-"

parseWhere :: Parser (Maybe ValueExpr)
parseWhere = optionMaybe (reserved "where" >>  parseValueExpr)
    
{-
-}

parseGroupBy :: Parser [ValueExpr]
parseGroupBy = pGroupBy <|> return []
  where
    pGroupBy = do
      _ <- reserved "group" >> reserved "by"
      many1 parseValueExpr
  
parseDirection :: Parser Direction
parseDirection 
  =   (reserved "asc" >> pure Asc)
  <|> (reserved "desc" >> pure Desc)
  <|> (pure Asc)

parseValueType :: Parser ValueType
parseValueType 
  =   (reserved "string" >> pure String)
  <|> (reserved "int" >> pure Int)
  <|> (reserved "float" >> pure Float)
  <|> (reserved "bool" >> pure Bool)

parseSortSpec :: Parser SortSpec
parseSortSpec = SortSpec <$> parseValueExpr <*> parseDirection
      

parseOrderBy :: Parser [SortSpec]
parseOrderBy = option [] $ do
    _ <- reserved "order"
    _ <- reserved "by"
    commaSep1 parseSortSpec

parseLimit :: Parser (Maybe ValueExpr)
parseLimit = optionMaybe (reserved "limit" >> parseValueExpr)

parseHaving :: Parser (Maybe ValueExpr)
parseHaving = pure Nothing
-- TODO

selectItem :: Parser (ValueExpr, Maybe Name)
selectItem = (,) <$> parseValueExpr <*> optionMaybe als
   where als = optional (reserved "as") *> identifier

parseSelectList :: Parser [(ValueExpr, Maybe Name)]
parseSelectList = commaSep1 selectItem <?> "selectList"


parseTableExpression :: Parser TableExpression
parseTableExpression = do
  TableExpression <$> parseFrom <*> parseWhere <*> parseGroupBy  <*> parseHaving <*> parseOrderBy <*> parseLimit

parseSelect :: Parser QueryExpr
parseSelect = do
  _ <- reserved "select" 
  sq <- parseSetQuantifier 
  sl <- parseSelectList
  mte <- optionMaybe parseTableExpression
  case mte of
    Just te -> return $ mkSelect sq sl te
    Nothing -> return $ mkSelect' sq sl


parseQueryExpr :: Parser QueryExpr
parseQueryExpr 
  = parseSelect
