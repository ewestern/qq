{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts  #-}
module Parse.Csv where

import Control.Monad ((>=>))
import Control.Applicative (optional)
import Control.Monad.Trans.Except
import Control.Monad.Error.Class



--import GHC.IO.Handle.FD
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as BS
import Data.Functor.Of
import Streaming
import qualified Streaming.Prelude as S


import Data.Either (isRight)

import qualified Data.Attoparsec.ByteString as AB
import Data.Attoparsec.ByteString.Char8 (endOfLine)
import qualified Data.ByteString.Streaming as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.Vector as V
import Data.Csv.Parser
import qualified Data.Csv.Streaming as CS
import Data.Traversable

import Streaming.Cassava hiding (Header)



import Sql.Syntax (TableRef(..))



-- import qualified System.IO.Streams as S
-- import qualified System.IO.Streams.Combinators as SC
-- import qualified System.IO.Streams.Attoparsec as SA
-- import qualified System.IO.Streams.Csv as CV

import Types

import Debug.Trace

data CsvError
  = ParseError CsvParseException
  | EndofStream ()

parseCsv :: MonadError CsvParseException m
          => Bool -- hasHeader
          -> B.ByteString m () 
          -> m (Maybe Header, Stream (Of RawRow) m ())
parseCsv header inputStream = 
  let is = decode NoHeader inputStream
  in 
    if header
      then do
        eitherPair <- S.next is 
        case eitherPair of
          Right (item, stream) -> return $ (Just item, stream)
          Left _ -> error "foobar"
      else return $ (Nothing, is)

