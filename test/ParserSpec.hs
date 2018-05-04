module ParserSpec where 
import Test.Hspec
import Sql.Parser
import Sql.Syntax
import Sql.Lexer (Parser)

import Text.Parsec (parse, eof)

run :: Parser a -> String -> a
run p s = case parse (p <* eof) ""  s of
    Left e -> error $ show e
    Right v -> v


spec :: Spec
spec = do
  describe "parsers" $ do
    it "parsers a basic select str" $ do
        run  parseQueryExpr "select '1'" `shouldBe` (basicSelectStr "1")
    it "parsers a basic select int" $ do
        run  parseQueryExpr "select 1" `shouldBe` (basicSelectInt 1)
    it "parses a simple path query" $ do
        let s = Select SQDefault [(Simple $ Star, Nothing)] [TablePath "foo.csv"] Nothing [] Nothing [] 
        run parseQueryExpr "select * from foo.csv" `shouldBe` s
