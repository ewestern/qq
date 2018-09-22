{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}

module Coercions where

import Control.Monad.Except
import Text.Read (readMaybe)
import Data.Char (toLower)

import Types
import Sql.Syntax

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
{-
numericConversionRules :: Prim -> Prim -> ErrR Prim
numericConversionRules s@(StringPrim _) other = coercePrim (Just (typeOf other)) s
numericConversionRules i@(IntPrim i1) other = case other of
  FloatPrim f -> return $ FloatPrim $ fromIntegral i1
  _ -> i
numericConversionRules a b = a
-}


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


