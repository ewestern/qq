{-# LANGUAGE DeriveDataTypeable #-}
module Sql.Syntax where

import Data.Data
import Data.Typeable


data Term
  = NumLit (Either Integer Double)
  | StringLit String
  | Star
  | BinOp Term BinOperator Term deriving (Eq, Show, Data, Typeable)


data ValueExpr
  = Simple Term deriving (Eq, Show, Data, Typeable)


data InPredValue
  = InList [ValueExpr]
  | InQueryExpr QueryExpr deriving (Eq, Show, Data, Typeable)


type Name = String

data Alias = Alias Name (Maybe [Name])  deriving (Eq, Show, Data, Typeable)

data BinOperator 
  = Times
  | Divide
  | Plus
  | Minus deriving (Eq, Show, Data, Typeable)

--newtype BinOperator = BinOperator [Name]
--newtype PrefixOperator = P

data SetQuantifier
  =  SQDefault
  | Distinct
  | All deriving (Eq, Show, Data, Typeable)


data TableRef
  = TablePath Name
  | StdIn deriving (Eq, Show, Data, Typeable)


data Direction
  = Asc
  | Desc deriving (Eq, Show, Data, Typeable)


data SortSpec = SortSpec ValueExpr Direction deriving (Eq, Show, Data, Typeable)


data CombineOp = Union | Except | Intersect deriving (Eq, Show, Data, Typeable)


basicSelectStr :: String -> QueryExpr
basicSelectStr s = Select SQDefault [(Simple $ StringLit s , Nothing)] [] Nothing [] Nothing [] 

basicSelectInt i = Select SQDefault [(Simple $ NumLit $ Left i , Nothing)] [] Nothing [] Nothing [] 

data QueryExpr
  = Select {
      _setQuantifier ::  SetQuantifier
    , _selectList :: [(ValueExpr, Maybe Name)]
    , _from :: [TableRef]
    , _where :: Maybe ValueExpr
    , _groupBy :: [ValueExpr]
    , _having :: Maybe ValueExpr
    , _orderBy :: [SortSpec]
  }
  | Combine QueryExpr CombineOp QueryExpr
  | With {
      _views :: [(Alias, QueryExpr)]
    , _queryExpr :: QueryExpr
  }
  | Values [[ValueExpr]]
  | Table [Name] deriving (Eq, Show, Data, Typeable)


