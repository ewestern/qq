module SqlSpec where 
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
  
  describe "SQL parsers" $ do
    it "parses a path" $ do
        run parsePath "foo.csv" `shouldBe` "foo.csv"
    it "parses [from] table ref" $ do
        run parseFrom "from foo.csv" `shouldBe` [TablePath "foo.csv" Nothing]
    it "parses a table Expression" $ do
        let te = TableExpression [TablePath "foo.csv" Nothing] Nothing [] Nothing [] Nothing
        run parseTableExpression "from foo.csv" `shouldBe` te
    it "parses a simple path query" $ do
        let s1 = Select SQDefault [(ValueExpr Star, Nothing)] [TablePath "foo.csv" Nothing] Nothing [] Nothing []  Nothing
        run parseQueryExpr "select * from foo.csv" `shouldBe` s1

    it "parses a simple condition query" $ do
        let s2 = Select SQDefault [(ValueExpr Star, Nothing)] [TablePath "foo.csv" Nothing] (Just $ ValueExpr (BinOp (Iden Nothing "name" Nothing) (Equality Equals) (StringLit "string" Nothing))) [] Nothing []  Nothing
        run parseQueryExpr "select * from foo.csv where name = 'string'" `shouldBe` s2
    it "parses a function application" $ do
        run parseFunctionApp "sum(column)" `shouldBe` (App FnSum [ValueExpr (Iden Nothing "column" Nothing) ] Nothing)
    it "parses a simple aggregate path query with columns" $ do
        let sl = [
                  (ValueExpr (Iden Nothing "column" Nothing), Nothing),
                  (ValueExpr (App FnSum [ValueExpr (Iden Nothing "val" Nothing)] Nothing), Nothing)]
            s = Select SQDefault sl [TablePath "foo.csv" Nothing] Nothing [ValueExpr ( Iden Nothing "column" Nothing) ] Nothing [] Nothing
        run parseQueryExpr "select column, sum(val) from foo.csv group by column" `shouldBe` s
    it  "parses a limit query" $ do
        let s = Select SQDefault [(ValueExpr (Iden Nothing "foo" Nothing), Nothing)] [TablePath "foo.csv" Nothing] Nothing [] Nothing [] (Just (ValueExpr (IntLit 6 Nothing) )) 
        run parseQueryExpr "select foo from foo.csv limit 6" `shouldBe` s
