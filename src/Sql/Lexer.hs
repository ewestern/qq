module Sql.Lexer where

import Text.Parsec
import qualified Text.Parsec.Token as Tok
import Text.Parsec.Language (emptyDef, LanguageDef)


type Parser = Parsec String ()


lexer = Tok.makeTokenParser $ emptyDef { 
      Tok.reservedNames = ["select", "from", "group", "by", "where", "having", "order", "in", "limit", "string", "int", "float", "bool", "type"]
    , Tok.reservedOpNames = ["(", ")", "+", "-", "/", "*" , ",", ";"]
    , Tok.caseSensitive = False
    , Tok.identStart = alphaNum <|> (oneOf "./_-")
    , Tok.identLetter = alphaNum <|> (oneOf "./_-")
  }


symbol :: String -> Parser String 
symbol = Tok.symbol lexer

comma :: Parser String 
comma = Tok.comma lexer

commaSep :: Parser a -> Parser [a]
commaSep = Tok.commaSep lexer

commaSep1 :: Parser a -> Parser [a]
commaSep1 = Tok.commaSep1 lexer

semi :: Parser String
semi = Tok.semi lexer

identifier :: Parser String 
identifier = Tok.identifier lexer <?> "identifier"

parens :: Parser a -> Parser a
parens = Tok.parens lexer

quotes :: Parser a -> Parser a 
quotes = between (char '"') (symbol "\"")

singleQuote :: Parser a -> Parser a
singleQuote = between (char '\'' ) (char '\'')

dot :: Parser String
dot = Tok.dot lexer

stringLiteral :: Parser String 
stringLiteral = Tok.stringLiteral lexer

whiteSpace :: Parser ()
whiteSpace = Tok.whiteSpace lexer

integer :: Parser Integer
integer = Tok.integer lexer

float :: Parser Double
float = Tok.float lexer

reserved :: String -> Parser ()
reserved = Tok.reserved lexer

reservedOp :: String -> Parser ()
reservedOp = Tok.reservedOp lexer

