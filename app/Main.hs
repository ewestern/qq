{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}

module Main where

import System.IO.Streams (InputStream, OutputStream, makeOutputStream, peek, connect, write)
import qualified System.IO.Streams.Combinators as SC
import qualified System.IO.Streams.List as SL
import System.IO.Streams.Handle (stdout)
import qualified Data.Vector as V
import qualified Data.Map as M
import Control.Monad.Except
import Control.Monad.Trans.State.Lazy
import Text.Parsec (parse, eof)
import Options.Applicative (execParser)
import System.IO (hPutStrLn, stderr)
import Data.Csv.Incremental (encodeRecord, encode)
import qualified Data.ByteString.Lazy.Char8 as BC
import qualified Data.ByteString.Char8 as BCS
import Control.Exception (throw)
import Debug.Trace

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

printer :: Maybe Row ->  IO ()
printer Nothing = return ()
printer (Just row) = BC.putStr $ encode $ encodeRecord $ V.map swallowError row
-- print (Just (Left err)) = return ()

swallowError :: ErrR Prim -> Prim
swallowError (Right p) = p
swallowError (Left e) = StringPrim "ERR"

printSink  :: IO (OutputStream Row)
printSink =  makeOutputStream printer

printHeader :: OutputStream BCS.ByteString -> Header -> IO ()
printHeader os h = write (Just $ prettify $ V.map (Right . StringPrim) h) os

prettify :: Row -> BCS.ByteString
prettify row = BC.toStrict $ encode $ encodeRecord $ V.map swallowError row



query :: Args -> IO ()
query a@(Args command options) = do
  let ctx = Context M.empty M.empty V.empty $ SelectHeaderMap M.empty
      queryExpr = run parseQueryExpr command
  init <- getInitial options queryExpr
-- TODO: figure out what to do with Row Errors!!
  eitherTuple <- runExceptT $ flip runStateT ctx ( plan (parallelism options) init queryExpr >>= execute) --   >>= liftIO . (SC.map (fmap throwEither)))
  -- sink <- stdout
  --traceShowM "map converted??"
  case eitherTuple of
    Right (input, context) -> do 
        printHeader stdout (_selectHeader context)
        pretty <- SC.map prettify input
        connect pretty stdout
    Left s -> error "ASD"

main :: IO ()
main = do
  args <- execParser parseOpts
  query args
  
