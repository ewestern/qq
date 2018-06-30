{-# LANGUAGE OverloadedStrings #-}
module Parse.Csv where

import Control.Monad ((>=>))
import Control.Applicative (optional)

--import GHC.IO.Handle.FD
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as BS

import Data.Either (isRight)

import qualified Data.Attoparsec.ByteString as AB
import Data.Attoparsec.ByteString.Char8 (endOfLine)



import qualified Data.Vector as V

import Data.Csv.Parser
import qualified Data.Csv.Streaming as CS
import Data.Traversable

import Sql.Syntax (TableRef(..), Prim(..))

import qualified Data.ByteString.Char8 as BC


import qualified System.IO.Streams as S
import qualified System.IO.Streams.Combinators as SC
import qualified System.IO.Streams.Attoparsec as SA
import qualified System.IO.Streams.Csv as CV

import Types

import Debug.Trace

parseCsv :: HasHeader -> S.InputStream BS.ByteString -> IO (Maybe Header, S.InputStream RawRow)
parseCsv True inputStream = do
    h <- parseHeader inputStream
    sr <- parseBody inputStream
    return $ ((Just h), sr)

parseCsv False tr = ((,) Nothing) <$> parseBody tr


comma = decDelimiter defaultDecodeOptions

parseHeader :: S.InputStream BS.ByteString -> IO (V.Vector String)
parseHeader st = fmap (fmap BC.unpack) $ SA.parseFromStream (header comma ) st 

parseRow :: AB.Parser (Maybe (V.Vector BS.ByteString))
parseRow = do
  ae <- AB.atEnd
  if ae 
    then return Nothing 
    else do
      r <- record comma
      _ <- optional endOfLine
      return $ Just r
  

parseBody :: S.InputStream BS.ByteString 
          -> IO (S.InputStream RawRow)
parseBody is =  CV.decodeStream CV.NoHeader is >>= SC.filter isRight >>= (SC.map (\(Right v) -> v))
 
