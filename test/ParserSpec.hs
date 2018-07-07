{-# LANGUAGE OverloadedStrings #-}
module ParserSpec where 
import Test.Hspec
import Parse
import Parse.Csv
import Sql.Syntax (TableRef(..))
import Types

import qualified System.IO.Streams.List as SL
import qualified Data.Vector as V

testCSV = "foo,bar,baz\npeter,1,2\nwas,3,4\nhere,5,6"

spec :: Spec
spec = do
  describe "CSV parsers" $ do
    it "parses a csv" $ do
      st <- readData (TableString testCSV)
      (mh, body) <- parseCsv True st
      rows <-  SL.toList body
      rows `shouldBe` fmap V.fromList [["peter","1","2"],["was","3","4"],["here","5","6"]]
      mh  `shouldBe` (Just $ V.fromList ["foo", "bar", "baz"])
