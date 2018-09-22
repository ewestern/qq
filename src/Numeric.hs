{-# LANGUAGE RankNTypes #-}
module Numeric where

import Types
import Coercions
import Sql.Syntax

class Coercible a => Numeric a where
  (+#) :: a -> a -> ErrR a
  (-#) :: a -> a -> ErrR a
  (*#) :: a -> a -> ErrR a
  (/#) :: a -> a -> ErrR a

instance Numeric Prim where
  (+#) = addPrim
  (/#) = dividePrim
  (*#) = multiplyPrim
  (-#) = subtractPrim


coercePrims :: Prim -> Prim -> ErrR (Prim, Prim)
coercePrims a b = case (a, b) of
  (IntPrim i1, IntPrim i2)      -> Right $ (a, b)
  (FloatPrim f1, FloatPrim f2)  -> Right $ (a, b)
  (StringPrim s, IntPrim i)     -> (,) <$> coercePrim (Just Int) a <*> (pure b)
  (IntPrim i, StringPrim s)     -> (,) <$> (pure a) <*> coercePrim (Just Int) b
  (StringPrim s, FloatPrim f1)  -> (,) <$> coercePrim (Just Float) a <*> (pure b)
  (FloatPrim f1, StringPrim s)  -> (,) <$> (pure b) <*> coercePrim (Just Int) b
  (StringPrim s1, StringPrim s2) ->  (,) <$> (coercePrim (Just Float) a) <*> (coercePrim (Just Float) b)
  (BoolPrim _, _)               -> Left $ TypeError "Cannot add Types" ""
  (_, BoolPrim _)               -> Left $ TypeError "Cannot add Types" ""
 

addPrim :: Prim -> Prim -> ErrR Prim
addPrim a b = do
  tup <- coercePrims a b
  case tup of
    (FloatPrim f1, FloatPrim f2) -> Right $ FloatPrim (f1 + f2)
    (IntPrim i1, IntPrim i2) -> Right $ IntPrim (i1 + i2)
    _ -> Left $ TypeError "Cannot add Types" ""


subtractPrim :: Prim -> Prim -> ErrR Prim
subtractPrim a b = do
  tup <- coercePrims a b
  case tup of
    (FloatPrim f1, FloatPrim f2) -> Right $ FloatPrim (f1 - f2)
    (IntPrim i1, IntPrim i2) -> Right $ IntPrim (i1 - i2)
    _ -> Left $ TypeError "Cannot subtract Types" ""

multiplyPrim :: Prim -> Prim -> ErrR Prim
multiplyPrim a b = do
  tup <- coercePrims a b
  case tup of
    (FloatPrim f1, FloatPrim f2) -> Right $ FloatPrim (f1 * f2)
    (IntPrim i1, IntPrim i2) -> Right $ IntPrim (i1 * i2)
    _ -> Left $ TypeError "Cannot multiply Types" ""

dividePrim :: Prim -> Prim -> ErrR Prim
dividePrim a b = do
  tup <- coercePrims a b
  case tup of
    (FloatPrim f1, FloatPrim f2) -> Right $ FloatPrim (f1 / f2)
    (IntPrim i1, IntPrim i2) -> Right $ IntPrim (i1 `div` i2)
    _ -> Left $ TypeError "Cannot divide Types" ""



{-
numOpPrim :: Num a => (a -> a -> a) -> Prim -> Prim -> ErrR Prim
numOpPrim f a b = do
  tup <- coercePrims a b
  case tup of
    (FloatPrim f1, FloatPrim f2) -> Right $ FloatPrim (f f1 f2)
    (IntPrim i1, IntPrim i2) -> Right $ IntPrim (f i1 i2)
    _ -> Left $ TypeError "Cannot perform operation on types; " ""
-}


{-
addPrim :: Prim -> Prim -> ErrR Prim
addPrim a b = case (a, b) of
  (IntPrim i1, IntPrim i2)      -> Right $ IntPrim (i1 + i2)
  (FloatPrim f1, FloatPrim f2)  -> Right $ FloatPrim (f1 + f2)
  (StringPrim s, IntPrim i)     -> coercePrim (Just Int) a >>= (addPrim b)
  (StringPrim s, FloatPrim f)   -> coercePrim (Just Float) a >>= (addPrim b)
  (BoolPrim _, _)               -> Left $ TypeError "Cannot add Types" ""
  _                             -> addPrim b a
-}
  

-- negatePrim :: Prim -> Prim -> ErrR Prim
-- negatePrim
{-
subtractPrim :: Prim -> Prim -> ErrR Prim
subtractPrim a b =  case (a, b) of
  (IntPrim i1, IntPrim i2)      -> Right $ IntPrim (i1 - i2)
  (FloatPrim f1, FloatPrim f2)  -> Right $ FloatPrim (f1 - f2)
  (StringPrim s, IntPrim i)     -> coercePrim (Just Int) a >>= (subtractPrim b)
  (StringPrim s, FloatPrim f)   -> coercePrim (Just Float) a >>= (subtractPrim b)
  (BoolPrim _, _)               -> Left $ TypeError "Cannot add Types" ""
  _                             -> addPrim b a
-}


{-
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
-}




