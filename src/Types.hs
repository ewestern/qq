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
-- import Data.Data
-- import Data.Typeable
import Data.Hashable
import Control.Exception (Exception, throw)
import GHC.Generics
import Data.Char (toLower)
import Text.Read (readMaybe)

import qualified Data.ByteString.Char8 as BC
import Control.Monad.Trans.State.Lazy

import System.IO.Streams (InputStream, OutputStream, makeOutputStream, peek)

import Data.Csv (ToField(..))

import Sql.Syntax
import qualified System.IO.Streams as S
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
    

class Foo a where
  
    
instance Hashable Prim

class Coercible a => Numeric a where
  (+#) :: a -> a -> ErrR a
  (-#) :: a -> a -> ErrR a
  (*#) :: a -> a -> ErrR a
  (/#) :: a -> a -> ErrR a

instance Numeric Prim where
  (+#) = addPrim
  (/#) = undefined


--
data SelectVal a where
  SelectPrim :: Prim -> SelectVal a
  -- TODO: we can probably replace the function with just the index.
  SelectCol :: Coercible a => (V.Vector a -> ErrR Prim) -> SelectVal a
  -- SelectCol :: Int -> SelectVal a
  SelectFunc :: Coercible a => Function -> [SelectVal a] -> SelectVal a
  SelectStar :: SelectVal a

instance Show (SelectVal a) where
  show (SelectPrim p) = "SelectPrim (" ++ (show p) ++ ")"
  show (SelectCol _) = "SelectCol"
  show (SelectFunc f ves) = "SelectFunc " ++ (show f) ++ (show ves)
  show SelectStar = "SelectStar"



type HasHeader = Bool

type E = StateT Context (ExceptT StaticError IO)

runE :: Context -> E a -> IO (Either StaticError (a, Context))
runE ctx em =  runExceptT $ flip runStateT ctx em

type Err a = Either Error a

type ErrS a = Either StaticError a

type ErrR a = Either RuntimeError a

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

class Coercible a where
  getCoercion :: (Maybe ValueType) -> a -> ErrR Prim
  toPrim ::  a -> ErrR Prim
  typeOf :: a -> ValueType

instance Coercible String where
  getCoercion = getStringCoercion'
  toPrim = return . StringPrim
  typeOf _ = String

instance Coercible Prim where
  getCoercion vt p = coercePrim vt p
  toPrim = return
  typeOf = \case
    IntPrim _ -> Int
    FloatPrim _  -> Float
    StringPrim _ -> String
    BoolPrim _ -> Bool

instance Coercible (ErrR Prim) where
  getCoercion vt ep = ep >>= (coercePrim vt)
  toPrim = id

-- String -> Other
numericConversionRules :: Prim -> Prim -> ErrR Prim
numericConversionRules s@(StringPrim _) other = coercePrim (Just (typeOf other)) s
numericConversionRules i@(IntPrim i1) other = case other of
  FloatPrim f -> return $ FloatPrim $ fromIntegral i1
  _ -> i
numericConversionRules a b = a



addPrim :: Prim -> Prim -> ErrR Prim
addPrim a b = do
  numericConversionRules
addPrim (IntPrim i1) (IntPrim i2) = Right $ IntPrim (i1 + i2)
addPrim (IntPrim i1) (FloatPrim f1) = Right $ FloatPrim (f1 + (fromIntegral i1))
addPrim f@(FloatPrim _) i@(IntPrim _) = addPrim i f
addPrim (FloatPrim f1) (FloatPrim f2) = Right $ FloatPrim (f1 + f2)
addPrim s@(StringPrim _) i@(IntPrim _) =  coercePrim (Just Int) s >>= (addPrim i)
addPrim i@(IntPrim _) s@(StringPrim _) = addPrim s i
addPrim s@(StringPrim _) f@(FloatPrim _) = coercePrim (Just Float) s >>= (addPrim f)
addPrim f@(FloatPrim _) s@(StringPrim _) = addPrim s f
addPrim _ _ = Left $ TypeError "Cannot add types: " ""


subtractPrim :: Prim -> Prim -> ErrR Prim
subtractPrim (IntPrim i1) (IntPrim i2) = Right $ IntPrim (i1 - i2)
subtractPrim (IntPrim i1) (FloatPrim f1) = Right $ FloatPrim ((fromIntegral i1) - f1)
subtractPrim (FloatPrim f1) (IntPrim i1) = Right $ FloatPrim (f1 - (fromIntegral i1))
subtractPrim (FloatPrim f1) (FloatPrim f2) = Right $ FloatPrim (f1 - f2)

subtractPrim s@(StringPrim _) i@(IntPrim _) =  coercePrim (Just Int) s >>= (flip subtractPrim i)
subtractPrim s@(StringPrim _) f@(FloatPrim _) = coercePrim (Just Float) s >>= (flip subtractPrim f)
subtractPrim i@(IntPrim _) s@(StringPrim _) = coercePrim (Just Int) s >>= (subtractPrim i)
subtractPrim f@(FloatPrim _) s@(StringPrim _) = undefined-- coercePrim (Just Int) s >>= (subtractPrim i)

subtractPrim _ _ = Left $ TypeError "Cannot add types: " ""





eitherIdx :: (Show a, MonadError RuntimeError m) => Int -> V.Vector a -> m a
eitherIdx i row = case (V.!?) row i of
  Just p -> return p
  Nothing -> throwError $ RowError i $ show row

coercePrim :: (MonadError RuntimeError m)
            => Maybe ValueType
            -> Prim
            -> m Prim
coercePrim Nothing = return
coercePrim (Just vt) = \case
  StringPrim s ->  getStringCoercion' (Just vt) s
  IntPrim i     -> getIntCoercion (Just vt) i
  FloatPrim f   -> getFloatCoercion (Just vt) f
  BoolPrim _    -> undefined


getFloatCoercion :: MonadError RuntimeError m => Maybe ValueType -> Double -> m Prim
getFloatCoercion Nothing f = return $ FloatPrim f
getFloatCoercion (Just vt) f = case vt of
    String -> return $ StringPrim $ show f
    Int -> return $ IntPrim $ floor f
    Float -> return $ FloatPrim f
    Bool -> throwError $ TypeError "asd" "dsd"


getIntCoercion :: MonadError RuntimeError m => Maybe ValueType -> Integer -> m Prim
getIntCoercion Nothing i = return $ IntPrim i
getIntCoercion (Just vt) i = case vt of
    String -> return $ StringPrim $ show i
    Int -> return $ IntPrim i
    Float -> return $ FloatPrim $ fromIntegral i
    Bool -> throwError $ TypeError "asd" "dsd"

getStringCoercion' :: MonadError RuntimeError m => Maybe ValueType -> String -> m Prim
getStringCoercion' Nothing s = return $ StringPrim s
getStringCoercion' (Just vt) s = case vt of
      String  -> return $ StringPrim  s
      Int     -> case readMaybe s of
        Just i -> return $ IntPrim i
        Nothing -> throwError $ TypeError "Expected Int: " s
      Float   -> case readMaybe s of
        Just f -> return $ FloatPrim f
        Nothing -> throwError $ TypeError "Expected Float: " s
      Bool  ->  case fmap toLower s of
          "true" -> return $ BoolPrim True
          "false" -> return $ BoolPrim False
          _       -> throwError $ TypeError "Expected Bool: " s


makeLenses ''RawContext
makeLenses ''Context
