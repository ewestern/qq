{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
module Sql.Syntax where

import qualified Data.ByteString as BS

import Control.Lens hiding (Context)
import GHC.Generics (Generic)
import Data.Hashable

import Data.Data
import Data.Typeable
import Control.Monad.Except


data Function
  = FnSum
  | FnAvg
  deriving (Eq, Ord, Show, Data, Typeable)

data Term
  = IntLit Integer (Maybe ValueType)
  | FloatLit Double (Maybe ValueType)
  | StringLit String (Maybe ValueType)
  | BoolLit Bool (Maybe ValueType)
  | Star
  | Iden (Maybe Name) Name (Maybe ValueType)
  | App Function [ValueExpr] (Maybe ValueType)
  | BinOp Term BinOperator Term deriving (Eq, Ord, Show, Data, Typeable)

newtype ValueExpr = ValueExpr {
  term :: Term
} deriving (Eq, Ord, Show, Data, Typeable)

--data ValueExpr = ValueExpr {
--    valueTerm :: Term 
--  , valueType :: Maybe (ValueType)
--} deriving (Eq, Ord, Show, Data, Typeable)
--

data InPredValue
  = InList [ValueExpr]
  | InQueryExpr QueryExpr deriving (Eq, Ord, Show, Data, Typeable)


type Name = String

data Alias = Alias Name (Maybe Name)  deriving (Eq, Ord, Show, Data, Typeable)

data BinOperator 
  = Equality EqualityOperator
  | Arithmetic ArithmeticOperator  deriving (Eq, Ord, Show, Data, Typeable, Generic)

instance Hashable BinOperator

data EqualityOperator
  = Equals
  | NotEquals deriving (Eq, Ord, Show, Data, Typeable, Generic)

instance Hashable EqualityOperator

data ArithmeticOperator
  = Times
  | Divide
  | Plus
  | Minus deriving (Eq, Ord, Show, Data, Typeable, Generic)

instance Hashable ArithmeticOperator

data SetQuantifier
  =  SQDefault
  | Distinct
  | All deriving (Eq, Ord, Show, Data, Typeable)


data TableRef
  = TablePath Name (Maybe Name)
  | TableString String
  | StdIn deriving (Eq, Ord, Show, Data, Typeable)


data Direction
  = Asc
  | Desc deriving (Eq, Ord, Show, Data, Typeable)

data ValueType
  = String
  | Int
  | Float
  | Bool deriving (Eq, Ord, Show, Data, Typeable)


data SortSpec = SortSpec {
  ssValue :: ValueExpr,
  ssDirection :: Direction
} deriving (Eq, Ord, Show, Data, Typeable)


data CombineOp = Union | Except | Intersect deriving (Eq, Ord, Show, Data, Typeable)


-- basicSelectStr :: String -> QueryExpr
-- basicSelectStr s = Select SQDefault [(ValueExpr (StringLit s) Nothing , Nothing)] [] Nothing [] Nothing [] Nothing
-- 
-- basicSelectInt i = Select SQDefault [(ValueExpr (IntLit i) Nothing , Nothing)] [] Nothing [] Nothing [] Nothing
-- 

data TableExpression
    = TableExpression
      { _teFrom :: [TableRef]
      , _teWhere :: Maybe ValueExpr
      , _teGroupBy :: [ValueExpr]
      , _teHaving :: Maybe ValueExpr
      , _teOrderBy :: [SortSpec]
      , _teLimit :: Maybe ValueExpr } deriving (Eq, Show, Data, Typeable)

type SelectList =  [(ValueExpr, Maybe Name)]

mkSelect :: SetQuantifier -> SelectList -> TableExpression -> QueryExpr
mkSelect sq sl (TableExpression f w g h o l) = Select sq sl f w g h o l

mkSelect' sq sl = Select sq sl [] Nothing [] Nothing [] Nothing

data QueryExpr
  = Select {
      _setQuantifier ::  SetQuantifier
    , _selectList :: SelectList
    , _from :: [TableRef]
    , _whereClause :: Maybe ValueExpr
    , _groupBy :: [ValueExpr]
    , _having :: Maybe ValueExpr
    , _orderBy :: [SortSpec]
    , _limit :: Maybe ValueExpr
  }
  | Combine QueryExpr CombineOp QueryExpr
  | With {
      _views :: [(Alias, QueryExpr)]
    , _queryExpr :: QueryExpr
  }
  | Values [[ValueExpr]]
  deriving (Eq, Ord, Show, Data, Typeable)

makeLenses ''QueryExpr
