{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase #-}
module Parse where

import Cli
import Sql.Lexer (Parser)
import Sql.Parser (parseQueryExpr)
import Parse.Csv
import Sql.Syntax (TableRef(..), Name, from, QueryExpr)
import Types

import Control.Lens( (^.), (%~), view, set, use, uses)

import Data.Maybe (fromMaybe, fromJust,catMaybes)
import qualified Data.Map as M
import qualified Data.Vector as V
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BC



getHeaderMap :: Maybe String -> Int -> Maybe Header -> RawHeaderMap
getHeaderMap alias i = \case
    Just h -> RawHeaderMap $ M.fromList $ fmap (\(i, a) -> ((alias, a), i) ) $ V.toList $ V.indexed h
    Nothing -> RawHeaderMap $ M.fromList $ zip (zip (repeat Nothing) $ take i genericColumns ) [0..]
    where
      genericColumns = fmap (uncurry (:)) $ zip (repeat 'c') (map show [1..])

getTableAlias :: TableRef -> Maybe Name
getTableAlias = \case
  TablePath p a -> a
  _             -> Nothing


makeContext :: TableRef -> Maybe Header -> S.InputStream RawRow -> IO RawContext
makeContext tableRef maybeHeader is = do
  maybeRow <- peek is
  let length = maybe 10 (V.length) maybeRow
      header =  fromMaybe (V.fromList $ fmap (uncurry (:)) $  zip (repeat 'c') (map show [1..])) maybeHeader
  return $ RawContext tableRef header $ getHeaderMap (getTableAlias tableRef) length maybeHeader


readData :: TableRef -> IO (S.InputStream BS.ByteString)
readData (TablePath fp _) = openFile fp ReadMode >>= S.handleToInputStream 
readData (TableString s) = SB.fromByteString $ BC.pack s
readData StdIn = return S.stdin

decompress :: Maybe Compression 
          -> S.InputStream BS.ByteString 
          -> IO (S.InputStream BS.ByteString )
decompress Nothing stream = return stream
decompress (Just ct) stream = case ct of
  ZLib -> Z.decompress stream
  GLib -> Z.gunzip stream


getInitial :: Options -> QueryExpr -> IO [(RawContext, S.InputStream RawRow)]
getInitial opts queryExpr = do
  let trefs =  queryExpr ^. from
  mapM (getInitial' opts) trefs

getInitial' :: Options -> TableRef -> IO (RawContext, S.InputStream RawRow)
getInitial' (Options{..}) tableRef = do
  stream <- readData tableRef >>= decompress compression
  case format of
    Csv -> do
      (mh, stream') <- parseCsv header stream
      ctx <- makeContext tableRef mh stream'
      return (ctx, stream')
      
