{-# LANGUAGE OverloadedStrings #-}
module ExecuteSpec where 
import Test.Hspec

import Cli
import Parse.Csv
import Sql.Syntax 
import Sql.Parser
import Parse
import Execute
import Plan
import Types
import Coercions
import Data.List (intercalate)

import qualified Data.Map as M
import qualified System.IO.Streams.List as SL
import System.IO.Streams (InputStream)
import qualified Data.Vector as V
import Control.Monad.Except
import Control.Monad.Trans.State.Lazy

import Text.Parsec (parse, eof)

doParse :: String -> QueryExpr
doParse s = case parse (parseQueryExpr <* eof) ""  s of
    Left e -> error $ show e
    Right v -> v

testCSV = "foo,bar,baz\npeter,1,2\nwas,3,4\nhere,5,6\nalabama,7,8\narkansas,9,10"

options = Options Nothing True Csv 1
headerlessOptions = Options Nothing False Csv 1

query' :: Options -> String -> IO (InputStream Row)
query' options command = do
  let ctx = Context M.empty M.empty (V.fromList ["foo", "bar", "baz"]) $ SelectHeaderMap M.empty
      queryExpr = doParse command
  init <- getInitial options queryExpr
  eitherStream <- runExceptT $ flip evalStateT ctx $ plan 1 init queryExpr >>= execute
  case eitherStream of
    Right input -> return $ input
    Left s -> error "ASD"



spec :: Spec
spec = do
  describe "Helpers" $ do
    it "updates Header Map" $ do
        pending
        let h1 = M.fromList [
              (Just "b", V.fromList ["foo", "bim", "bop" ]),
              (Just "c", V.fromList ["foo", "bar", "baz" ])] 
            m1 = updateHeaderMap h1  [ BinOp (IntLit 1 Nothing) (Arithmetic Plus) (IntLit 3 Nothing), Star]
            expected = M.fromList [
              ((Just "c", "foo"), 1), ((Just "c", "bar"), 2), ((Just "c", "baz"), 3),
              ((Just "b", "foo"), 4), ((Just "b", "bim"), 5), ((Just "b", "bop"), 6)]
        m1 `shouldBe` (SelectHeaderMap expected)
    it "coerces a string into a primitive"$ do
        let c1 = getCoercion Nothing ("1"::String)
        c1 `shouldBe` (Right $ StringPrim "1")
        let c2 = getCoercion (Just Int) ("1"::String)
        c2 `shouldBe` (Right $ IntPrim 1)

  describe "Execution" $ do
    it "Tests Limits" $ do
      let s = "select * from test/resources/test1.csv limit 2"
      outputRow <- query' options s
      ls <- SL.toList outputRow
      length ls `shouldBe` 2
    it "tests order by" $ do
      let s =  "select num_failures as type int from test/resources/test1.csv order by num_failures"
      outputRow <- query' options s
      ls <- SL.toList outputRow
      let expected = (fmap ((flip V.cons V.empty) . Right . IntPrim)) [21, 24, 28, 41, 49, 80, 134, 139, 144]
      ls `shouldBe` expected
    it "tests aggregate functions" $ do
      let s = "select script, sum(num_failures) from test/resources/test1.csv group by script"
      outputRow <- query' options s
      ls <- SL.toList outputRow
      (length ls) `shouldBe` 6

    it "tests filters" $ do
      let s = "select script, num_failures from test/resources/test1.csv where script = 'check_disk.sh'"
      outputRow <- query' options s
      ls <- SL.toList outputRow
      (length ls) `shouldBe` 2

    it "tests anonymous columns" $ do
      let s = "select c1, c2 from test/resources/test2.csv where c1 = 'check_disk.sh'"
      outputRow <- query' headerlessOptions s
      ls <- SL.toList outputRow
      (length ls) `shouldBe` 2


showTable :: Show a => [V.Vector a] -> String
showTable [] = ""
showTable (x:xs) = (show x) ++ ("\n") ++ (showTable xs)
