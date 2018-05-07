{-# LANGUAGE ScopedTypeVariables #-}
{-#LANGUAGE LambdaCase #-}
{-#LANGUAGE FlexibleContexts #-}
{-#LANGUAGE FlexibleInstances #-}

module Plan where

import Debug.Trace
import Data.Bifunctor (first)
import Control.Monad.Trans.State.Lazy
import Control.Exception (Exception, throw)
import Control.Lens( (^.), (%~), view, set, use, uses)
import Control.Monad.Except
import qualified Data.Vector as V
import qualified Data.Map as M
import Data.Maybe (fromMaybe, fromJust,catMaybes)
import System.IO.Streams (InputStream)


import Sql.Syntax
import Parse.Csv
import Types

newtype Plan = Plan {
    node :: Node
}

type Condition = Row -> ErrR Bool
type RawCondition = RawRow -> ErrR Bool


data Node
  = Limit Node Int
  | SeqScan SelectList RawCondition TableRef (InputStream RawRow)
  | Sort Node [SortSpec]
  | HashAggregate Node SelectList [ValueExpr] Condition
  | NestedLoop Node Node Condition
  | HashJoin Node Node Condition
  | MergeJoin Node Node Condition
  | ParallelAggregate Node Condition
  | Eval SelectList -- doesn't use stream

instance Show Node where
  show (Limit n _) = "Limit ( " ++ show n ++ ")"
  show (SeqScan _ _ tr _) = "SeqScan(" ++ (show tr) ++ ")" 
  show (Sort n _) = "Sort(" ++ (show n) ++ ")"
  show _ = "ASD"

throwEither :: ErrS a -> a
throwEither (Left e) = throw e
throwEither (Right v) = v

getCondition :: (Show a, Coercible a) => HeaderMap -> Maybe ValueExpr -> ErrS (V.Vector a -> ErrR Bool)
getCondition _ Nothing = return $ const (return True)
getCondition hm  (Just ve) = do
  sv <- evaluateTerm hm $ term ve
  case sv of
    SelectPrim (BoolPrim b) -> return $ const $ return b
    SelectPrim p -> throwError $ WhereError (show ve)
    SelectCol f -> return $ \row -> do
        p <- f row
        case p of
            BoolPrim b -> return b
            _ -> throwError $ TypeError "asd" (show ve)
    SelectFunc f ves -> undefined
    _ -> throwError $ WhereError (show ve)

evaluateTerm :: (Show a, Coercible a)
            => HeaderMap 
            -> Term
            -> ErrS (SelectVal a)
evaluateTerm headerMap term' =  case term' of
  IntLit e c -> fmap SelectPrim (fixError $ getCoercion c $ IntPrim e)
  StringLit s c -> fmap SelectPrim (fixError $ getCoercion c $ StringPrim s)
  FloatLit f c -> fmap SelectPrim (fixError $ getCoercion c $ FloatPrim f)
  Iden mQual name c -> 
      case M.lookup (mQual, name) headerMap of
          Just i ->  return $ SelectCol $ eitherIdx i >=> (getCoercion c) 
          Nothing -> throwError $ ColumnError name

  App func valueExprs c -> do
    -- TODO: Coercion?
    svs <- mapM (evaluateTerm headerMap) $ fmap term valueExprs
    return $ SelectFunc func svs

  Star -> return SelectStar
  BinOp t1 op t2 ->  do
      sv1 <- evaluateTerm headerMap t1
      sv2 <- evaluateTerm headerMap t2
      evaluateBinOp op sv1 sv2
  where
    fixError :: Either e Prim -> ErrS Prim 
    fixError = first (const $ CoercionError "asd")

  -- SelectFunc :: Coercible a => Function -> [SelectVal a] -> SelectVal a
evaluateBinOp :: MonadError StaticError m => BinOperator -> SelectVal a -> SelectVal a -> m (SelectVal a)
evaluateBinOp bop (SelectPrim p1) (SelectPrim p2) = SelectPrim <$> evaluateBinOpPrim bop p1 p2
evaluateBinOp bop (SelectCol f) (SelectPrim p) = 
    return $ SelectCol $ \r -> f r >>= (\p1 -> first (const $ TypeError "asd" "dsa") $ evaluateBinOpPrim bop p1 p)
-- Order of operation matters!
evaluateBinOp bop (SelectPrim p) (SelectCol f) =
    return $ SelectCol $ \r -> do
        prim <- f r
        first (const $ TypeError "asd" "dsa") $ evaluateBinOpPrim bop p prim
evaluateBinOp bop SelectStar x = throwError $ OperatorError bop
evaluateBinOp bop x SelectStar = throwError $ OperatorError bop
    
evaluateBinOpPrim :: MonadError StaticError m => BinOperator -> Prim -> Prim -> m Prim
evaluateBinOpPrim (Arithmetic op) (IntPrim a) (IntPrim b) = return $ IntPrim $ (getArithFunction op) a b
evaluateBinOpPrim (Arithmetic op) (FloatPrim a) (FloatPrim b) = return $ FloatPrim $ (getArithFunction op) a b
evaluateBinOpPrim (Equality op) (IntPrim a) (IntPrim b) = return $ BoolPrim $ (getEqualFunctions op) a b
evaluateBinOpPrim (Equality op) (FloatPrim a) (FloatPrim b) = return $ BoolPrim $ (getEqualFunctions op) a b
evaluateBinOpPrim (Equality op) (StringPrim a) (StringPrim b) = return $ BoolPrim $ (getEqualFunctions op) a b

evaluateBinOpPrim op a b  = throwError $ OperatorError op

getArithFunction Plus = (+)
getArithFunctions Times = (*)
getArithFunctions Divide = (/)
getArithFunctions Minus = (-)

getEqualFunctions Equals = (==)
getEqualFunctions NotEquals = (/=)



updateContext :: TableRef -> RawContext -> Context -> Context
updateContext tr rc  = rawContexts %~ (M.insert tr rc)



plan :: [(RawContext, InputStream RawRow)] -> QueryExpr -> E Plan
-- seems like we should be able to generalize this away
plan _ (Select _ sl [] _ _ _ _ _) = undefined 

-- a select on a single table
plan [(rc, stream)] (Select _ sl [tr] w [] Nothing [] Nothing) = do
    modify (updateContext tr rc)
    let headerMap = unRawHeaderMap $ rc ^. rawHeaderMap
        condition = throwEither $ getCondition headerMap w
    return $ Plan (SeqScan sl condition tr stream)

plan h (Select sq selectList tables whereClause groupBys having orders (Just ve)) = do
  (Plan node) <- plan h $ Select sq selectList tables whereClause groupBys having orders Nothing
  selectHeaderMap <- gets $ flip (^.) selectHeaderMap
  case evaluateTerm (unSelectHeaderMap selectHeaderMap) $ term ve of
    Right ((SelectPrim (IntPrim i) )::SelectVal Prim) -> return $ Plan (Limit node $ fromIntegral i) 
    _   -> throw $ TypeError "Invalid LIMIT expression." (show ve )

plan h (Select sq selectList tables whereClause groupBys having order@(o:os) Nothing) = do
  (Plan node) <- plan h $ Select sq selectList tables whereClause groupBys having [] Nothing
  return $ Plan (Sort node order) 
  
plan h (Select sq selectList tables whereClause groupingKeys having [] Nothing) = do
  (Plan node) <- plan h $ Select sq selectList tables whereClause [] having [] Nothing
  selectHeaderMap <- gets $ flip (^.) selectHeaderMap
  let condition = throwEither $ getCondition (unSelectHeaderMap selectHeaderMap) having
  return $ Plan (HashAggregate node selectList groupingKeys condition)

plan _ _ = undefined


