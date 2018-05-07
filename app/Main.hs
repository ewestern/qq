{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import System.IO.Streams (InputStream, OutputStream, makeOutputStream, peek, connect)
import qualified Data.Vector as V
import qualified Data.Map as M
import Control.Monad.Except
import Control.Monad.Trans.State.Lazy
import Text.Parsec (parse, eof)
import Options.Applicative (execParser)

import Execute
import Cli
import Parse
import Types
import Plan
import Sql.Lexer (Parser)
import Sql.Parser (parseQueryExpr)


run :: Parser a -> String -> a
run p s = case parse (p <* eof) "Command-line SQL"  s of
    Left e -> error $ show e
    Right v -> v


type Delimiter = Char

showPrimRow :: Delimiter -> Row  -> String
showPrimRow c v = (show $ V.head v) ++ (V.foldl (\s p -> c:(s ++ (show p))) "" $ V.tail v)


printer :: Delimiter -> Maybe Row ->  IO ()
printer _ Nothing = return ()
printer c (Just v) = print $ showPrimRow c v



printSink  :: IO (OutputStream Row)
printSink =  makeOutputStream $ printer ','


query :: Args -> IO ()
query a@(Args command options) = do
  let ctx = Context M.empty M.empty Nothing $ SelectHeaderMap M.empty
      queryExpr = run parseQueryExpr command
  init <- getInitial options queryExpr
  eitherStream <- runExceptT $ flip evalStateT ctx $ plan init queryExpr >>= execute
  sink <- printSink
  case eitherStream of
    Right input -> connect input sink
    Left s -> error "ASD"



main :: IO ()
main = do
  args <- execParser parseOpts
  query args
  
