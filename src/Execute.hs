{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-#LANGUAGE TypeFamilies #-}
{-#LANGUAGE TypeSynonymInstances #-}
{-#LANGUAGE FlexibleInstances #-}
{-#LANGUAGE FlexibleContexts #-}
{-#LANGUAGE LambdaCase #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}






module Execute where

import Debug.Trace
import Data.Data
import Data.Typeable
import Data.Hashable


import Control.Lens( (^.), (%~), view, set, use, uses)
import Control.Monad.Trans.Except (ExceptT(..), except)

import Data.Either
import Data.Maybe (fromMaybe, fromJust,catMaybes)
import Data.Tuple (swap)
import Data.List (sortBy, partition, (\\))
import Control.Exception (Exception, throw)
-- import Control.E
import qualified Data.HashMap as HM
import qualified Data.Vector as V
import qualified Data.Bifunctor as BF
import qualified Data.Map as M
import qualified Data.ByteString.Lazy as BL
import qualified Data.Set as S
import Control.Monad.Except
import Control.Applicative (liftA2)
import Control.Monad
import Control.Monad.Trans.State.Lazy

import System.IO.Streams (InputStream, OutputStream, makeOutputStream, peek)
import qualified System.IO.Streams.Combinators as SC
import qualified System.IO.Streams.List as SL

import Plan
import Sql.Syntax
import Types
import Parse


type Key = [Maybe (ErrR Prim)]

type AggMap = HM.Map Key AggRow




evaluateColumn :: Coercible a => HeaderMap -> SelectVal a -> V.Vector a -> Row
evaluateColumn headerMap sv row = do
  case sv of
    SelectPrim p -> V.singleton $ return p
    -- TODO: do a coercion here if necessary
    SelectCol f -> V.singleton $ f row
    SelectFunc _ vfs -> (join . V.fromList) $ fmap (\sv ->  evaluateColumn headerMap sv row) vfs
    SelectStar -> V.map toPrim row

evaluateSingleRow :: HeaderMap -> [ValueExpr] -> ErrS (RawRow -> Row)
evaluateSingleRow headerMap ves = do
  svs <- mapM (evaluateTerm headerMap) $ fmap term ves
  return $ \rawRow -> foldl (folder rawRow) V.empty svs
  where
    folder :: RawRow -> Row -> SelectVal String -> Row
    folder rawRow vec sv = vec V.++ (evaluateColumn headerMap sv rawRow)

validateArgumentList :: Monad m => Function -> [a] -> ExceptT StaticError m ()
validateArgumentList func ls = case func of
  FnSum -> if length ls == 1 then return () else throwError err
  FnAvg -> if length ls == 1 then return () else throwError err
  where
      err = FunctionError "Incorrect number of arguments." (show func)

getSortingFunction :: [(SelectVal (ErrR Prim), Direction)] -> Row -> Row -> Ordering
getSortingFunction [] = \_ _ -> EQ
getSortingFunction ((sv, dir):xs)  =
  case sv of
    SelectCol f -> \or1 or2 -> sortFunc dir f (getSortingFunction xs) or1 or2
    _ -> \ _ _ -> EQ


applyDirection :: Direction -> Ordering -> Ordering
applyDirection Asc = id
applyDirection Desc = \case
  LT -> GT
  GT -> LT
  EQ -> EQ

sortFunc  :: Direction -> (Row -> ErrR Prim) -> (Row -> Row -> Ordering) -> Row -> Row -> Ordering
sortFunc dir  f g = 
  let 
    h or1 or2 = case compare (f or1) (f or2) of
      LT -> applyDirection dir LT
      GT -> applyDirection dir GT
      EQ -> g or1 or2
  in h


-----

updateHeaderMap :: M.Map (Maybe Name) Header -> [Term] -> SelectHeaderMap
updateHeaderMap rawHeader = SelectHeaderMap . snd . foldl (updateHeaderItem rawHeader)  (0, M.empty) . (zip [0..])

updateHeaderItem :: M.Map (Maybe Name) Header -> (Int, HeaderMap) -> (Int, Term) -> (Int, HeaderMap)
-- we really only need the header for * terms.
updateHeaderItem rawHeaders (i, hm) (j, term') = 
  foldl func (0, hm) $ zip [(i+j)..] $ columnNamesFromTerm term'
  where
    columnNamesFromTerm  :: Term -> [Maybe (Maybe Name, String)]
    columnNamesFromTerm term'' =  case term'' of
      Star -> M.foldlWithKey (\acc k v -> acc ++ (fmap (\s -> Just (k, s)) $ V.toList v) ) [] rawHeaders
      Iden mq n _ -> [Just (mq, n)]
      App fn vxs _ -> join $ fmap (columnNamesFromTerm . term) vxs
      _ -> [Nothing]
    func (_, hm) (k, Just tup) =  (k, M.insert tup k hm)
    func acc _  = acc



-- TODO: eventually want to partition Lefts into a separate stream
collapsePredicate :: ErrR Bool -> Bool
collapsePredicate (Right v) = v
collapsePredicate _ = False


execute :: Plan -> E (InputStream Row)
execute (Plan n) = execute' n

execute' :: Node -> E (InputStream Row)
execute' (SeqScan selectList cond tr inputStream)  = do
  (Just rc) <- gets $ M.lookup tr . flip (^.) rawContexts 
  filteredStream <- liftIO $ SC.filter (collapsePredicate . cond) inputStream
  let valueExpressions = fmap fst selectList
      headerMap = unRawHeaderMap $ rc ^. rawHeaderMap
      evaluator = throwEither $ evaluateSingleRow headerMap valueExpressions
  evaluatedStream <- liftIO $ SC.map evaluator filteredStream
  rcs <- use rawContexts
  let rawHeaders = M.foldlWithKey (\acc k rc -> M.insert (getTableAlias k) (rc ^. rawHeader) acc) M.empty rcs
      selectHeaders = updateHeaderMap rawHeaders $ fmap term valueExpressions
  modify (set selectHeaderMap selectHeaders )
  -- TODO!
  return evaluatedStream

execute' (Limit node i) = execute' node >>= (liftIO . SC.take (fromIntegral i))

execute' (Sort node ss) = do
  ls <- execute' node >>= (liftIO . SL.toList)
  selectMap <- uses selectHeaderMap unSelectHeaderMap
  let svs = throwEither $ mapM (evalSortSpec selectMap) ss
  -- TODO
  liftIO $ SL.fromList $ sortBy (getSortingFunction svs) ls
  where
    evalSortSpec hm (SortSpec val dir) = do
      sv <- evaluateTerm hm $ term val
      return (sv, dir)

execute' (HashAggregate node selectList groupingKeys@(k:ks) cond) = do
  evaluated <- execute' node
  selectMap <- uses selectHeaderMap unSelectHeaderMap
  selectVals <- liftEither $ mapM (evaluateTerm selectMap) $ fmap (term . fst) selectList
  groupVals <- liftEither $ mapM (evaluateTerm selectMap) $ fmap term groupingKeys
  lift $ validateAggregateQuery selectList groupingKeys
  aggregator <- liftEither $ getAggregator selectMap selectVals groupVals
  aggMap <- liftIO $ SC.fold aggregator HM.empty evaluated
  liftIO $ (SL.fromList $ HM.toList aggMap) >>= SC.map (resolveAggRow selectVals)

resolveAggRow :: [SelectVal (ErrR Prim)] -> (Key, AggRow) -> Row
resolveAggRow selectVals (key, aggRow) = V.fromList $ catMaybes $ fmap resolve $ zip [0..] selectVals
  where
    resolve (i, selectVal) = case selectVal of
         SelectFunc funcName _ ->
            if S.member funcName aggregateFunctions
              then fmap resolveAggregation $  aggRow V.! i
              else  key !! i
         _ -> key !! i


resolveAggregation :: Aggregation -> ErrR Prim
resolveAggregation = \case
  Default p -> p

aggregateFunctions = S.fromList
  [FnSum, FnAvg ]

isAggregateTerm :: Term -> Bool
isAggregateTerm = \case
  App function vxs _ -> S.member function aggregateFunctions
  _ -> False


isAggregateSelectFunc :: SelectVal a -> Bool
isAggregateSelectFunc = \case
  SelectFunc func _ -> S.member func aggregateFunctions
  _ -> False

validateAggregateQuery :: Monad m => SelectList -> [ValueExpr] -> ExceptT StaticError m ()
validateAggregateQuery selectList groupBys =
  let (_, nonAggregates) = partition (isAggregateTerm . term) $ fmap fst selectList
  in
      case S.null $ (S.fromList nonAggregates) S.\\ (S.fromList groupBys) of
        True -> return ()
        _       -> throwError $ GroupError "asd"

numericOpPrim :: (MonadError RuntimeError m ) => (forall n. Num n => n -> n -> n) -> (Prim, Prim) -> m Prim
numericOpPrim func = \case
  (IntPrim i1, IntPrim i2) -> return $ IntPrim $ func i1 i2
  (IntPrim i, FloatPrim f) -> return $ FloatPrim $ func (fromIntegral i) f
  (FloatPrim f, IntPrim i) -> return $ FloatPrim $ func f (fromIntegral i)
  (FloatPrim f1, FloatPrim f2) -> return $ FloatPrim $ func f1 f2
  (a, b) -> throwError $ TypeError "Tried to perform a numeric operation on: " (show a ++ " " ++ show b)


getSumAggregator :: [SelectVal (ErrR Prim)] -> ErrS (Aggregation -> Row -> Aggregation)
-- should handle all Select* cases.
getSumAggregator (sv:[]) = case sv of
  SelectPrim p -> undefined
  SelectCol f -> return $ closure f
    where
    closure f (Default eitherPrim) row = Default $ do
      prim1 <- f row >>= (getCoercion (Just Float))
      prim2 <- eitherPrim >>= (getCoercion (Just Float))
      numericOpPrim (+) (prim1, prim2)
  SelectFunc a b -> undefined -- ???
  SelectStar -> undefined

getSumAggregator _ = throwError $ FunctionError "ad" "dsa"


getColumnAggregator :: SelectVal (ErrR Prim) -> ErrS (Aggregation -> Row -> Aggregation)
getColumnAggregator (SelectFunc funcType selectVals) = case funcType of
  FnSum -> getSumAggregator selectVals
  FnAvg -> error $ "ASD??" ++ (show funcType)
getColumnAggregator selectVal = do
  return $ \agg row -> agg




-- evaluateColumn :: Coercible a => HeaderMap -> SelectVal a -> V.Vector a -> Row
getKey ::[SelectVal (ErrR Prim)] -> Row -> Key
getKey groupVals row =  fmap build groupVals
  where
      build = \case
        SelectPrim p -> Just (return p)
        SelectCol f -> Just $ f row
        SelectFunc f ves ->
          if S.member f aggregateFunctions
            then Nothing
            else  error "ASD?"
        _ -> error "ASD"

getFunctionAggregation :: ErrR Prim -> Function -> Aggregation
getFunctionAggregation prim = \case
  FnSum -> Default prim

getAggregationRow :: [SelectVal (ErrR Prim)] -> Row -> AggRow
getAggregationRow selectVals row = fmap func $ V.zip (V.fromList selectVals) row
  where
    func (selectVal, eitherPrim) = case selectVal of
        SelectFunc function _ ->
            if S.member function aggregateFunctions
            then Just (getFunctionAggregation eitherPrim function)
            else Nothing
        _ -> Nothing

getRowAggregator :: [SelectVal (ErrR Prim)]
                  -> ErrS (AggRow -> Row -> AggRow)
getRowAggregator selectVals = do
  colAggs <- fmap V.fromList $ mapM getColumnAggregator selectVals
  let closure :: AggRow -> Row -> AggRow
      closure accRow row = fmap (go row) $ V.zip accRow colAggs
  return $ closure
  where
    go :: Row -> (Maybe Aggregation, (Aggregation -> Row -> Aggregation)) -> (Maybe Aggregation)
    go row (maybeAgg, f) = case maybeAgg of
      Just agg ->  Just $ f agg row
      Nothing -> Nothing

getAggregator :: HeaderMap -> [SelectVal (ErrR Prim)] -> [SelectVal (ErrR Prim)]  -> ErrS (AggMap -> Row -> AggMap)
getAggregator headerMap selectVals groupVals = do
    aggregator <- getRowAggregator selectVals
    return $ closure groupVals aggregator
    where
      closure :: [SelectVal (ErrR Prim)] -> (AggRow -> Row -> AggRow) -> AggMap -> Row -> AggMap
      closure groupVals aggregator aggMap row =
          let key = getKey groupVals row
          in case HM.lookup key aggMap of
              Just v -> HM.insert key (aggregator v row) aggMap
              -- TODO: Not sure this logic (ignoring Left) is what we want.
              _ ->  let val = getAggregationRow selectVals row
                    in HM.insert key val aggMap
