{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE LambdaCase #-}


module Types where

import Control.Lens hiding (Context)
import Control.Arrow
import Data.Hashable
import Control.Exception (Exception, throw)
import GHC.Generics
import Data.Char (toLower)
import Text.Read (readMaybe)

import qualified Data.ByteString.Char8 as BC
import Control.Monad.Trans.State.Lazy

import Data.Csv (ToField(..))

import Sql.Syntax
-- import Coercions
import qualified Data.Vector as V
import qualified Data.Map as M
import Control.Monad.Except

  


data Prim
  = IntPrim Integer  
  | FloatPrim Double
  | StringPrim String
  | BoolPrim Bool deriving (Eq, Show, Ord, Generic)

instance ToField Prim where
  toField = \case 
    IntPrim i -> BC.pack $ show i
    FloatPrim f -> BC.pack $ show f
    StringPrim s -> BC.pack s
    BoolPrim b -> BC.pack $ show b
    

instance Hashable Prim

--


type E = StateT Context (ExceptT StaticError IO)

runE :: Context -> E a -> IO (Either StaticError (a, Context))
runE ctx em =  runExceptT $ flip runStateT ctx em

type Err a = Either Error a

type ErrS a = Either StaticError a

type ErrR a = Either RuntimeError a

liftStaticError :: ErrS a -> Err a
liftStaticError = left StaticError

liftRuntimeError :: ErrR a -> Err a
liftRuntimeError = left RuntimeError

data RuntimeError
  = RowError Int String
  | TypeError String String deriving (Eq, Ord, Generic, Show)

instance Hashable RuntimeError

instance Exception RuntimeError

data StaticError
  = OperatorError BinOperator
  | WhereError String
  | GroupError String
  | ColumnError String
  | CoercionError String
  | FunctionError String String deriving (Eq, Ord, Generic, Show)

instance Hashable StaticError

instance Exception StaticError

data Error
  = RuntimeError RuntimeError
  | StaticError StaticError deriving (Eq, Ord, Generic, Show)

instance Hashable Error

instance Exception Error

type Row = V.Vector (ErrR Prim)

type RawRow = V.Vector String

type AggRow = V.Vector (Maybe Aggregation)

data Aggregation
  = Default (ErrR Prim)
  | Average (ErrR Prim, ErrR Prim)


type Header = V.Vector String

-- Use types to distinguish where important, otherwise don't
type HeaderMap = M.Map (Maybe String, String) Int

newtype SelectHeaderMap = SelectHeaderMap {
  unSelectHeaderMap :: HeaderMap
} deriving (Eq, Show)

newtype RawHeaderMap = RawHeaderMap {
  unRawHeaderMap :: HeaderMap
} deriving (Eq, Show)

data RawContext = RawContext {
    _tableRef :: TableRef
  , _rawHeader  :: Header
  , _rawHeaderMap :: RawHeaderMap
}

data Context = Context {
    _rawContexts :: M.Map TableRef RawContext
  , _tableAlias :: M.Map Name TableRef
  , _selectHeader :: Header
  , _selectHeaderMap :: SelectHeaderMap
}





eitherIdx :: (Show a, MonadError RuntimeError m) => Int -> V.Vector a -> m a
eitherIdx i row = case (V.!?) row i of
  Just p -> return p
  Nothing -> throwError $ RowError i $ show row


makeLenses ''RawContext
makeLenses ''Context
