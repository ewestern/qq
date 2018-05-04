module Sql.Parser where

import Sql.Lexer
import Sql.Syntax

import Text.Parsec 
import Text.Parsec.Expr


parseBinaryOperator :: Parser BinOperator
parseBinaryOperator 
    = reservedOp "+" *> pure Plus
    <|> reservedOp "-" *> pure Minus
    <|> reservedOp "/" *> pure Divide
    <|> reservedOp "*" *> pure Times
    

parseTerm :: Parser Term
parseTerm = StringLit <$> parseStringLiteral
  <|> NumLit <$> number 
  <|> string "*" *> pure Star

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
parseValueExpr = Simple <$> (buildExpressionParser table parseTerm)

-- TODO
parseSetQuantifier  :: Parser SetQuantifier
parseSetQuantifier = pure SQDefault

parsePath :: Parser String
parsePath = many1 (oneOf "/." <|> alphaNum)

parseFrom :: Parser [TableRef]
parseFrom = do
  _ <- reserved "from"
  commaSep1 tref
  where
    tref  = TablePath <$> parsePath
          <|> pDash *> (pure StdIn)
    pDash = reservedOp "-"

parseWhere :: Parser (Maybe ValueExpr)
parseWhere = pure Nothing

parseGroupBy :: Parser [ValueExpr]
parseGroupBy = pure []

parseOrderBy :: Parser [SortSpec]
parseOrderBy = pure []

parseHaving :: Parser (Maybe ValueExpr)
parseHaving = pure Nothing
-- TODO

selectItem :: Parser (ValueExpr,Maybe Name)
selectItem = (,) <$> parseValueExpr <*> optionMaybe als
   where als = optional (reserved "as") *> identifier

selectList :: Parser [(ValueExpr,Maybe Name)]
selectList = commaSep1 selectItem


     

parseSelect :: Parser QueryExpr
parseSelect = 
  reserved "select" *> (Select <$> parseSetQuantifier <*> selectList <*> parseFrom <*> parseWhere <*> parseGroupBy  <*> parseHaving <*> parseOrderBy)
    
  

parseQueryExpr :: Parser QueryExpr
parseQueryExpr 
  = parseSelect
