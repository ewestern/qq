{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}

module Cli where

-- import Options.Generic

import Control.Applicative
import Options.Applicative
import Data.Monoid ((<>))


data Compression
  = GLib
  | ZLib deriving (Show, Read)

data Format
  = Csv deriving (Show, Read)



data Options
  = Options {
  compression :: Maybe Compression,
  header :: Bool,
  format :: Format,
  parallelism :: Int
} deriving (Show, Read)

data Args
  = Args String Options deriving (Show)

parseFormat :: Parser Format
parseFormat = pure Csv

parseGlib :: Parser (Maybe Compression)
parseGlib = flag Nothing (Just GLib ) (long "glib" <> short 'g' <> help "Decompress stream using glib.")

parseZlib :: Parser (Maybe Compression)
parseZlib =  flag Nothing (Just ZLib) (long "zlib" <> short 'z' <> help "Decompress stream using zlib.") 

parseCompression :: Parser (Maybe Compression)
parseCompression 
  = parseGlib
  <|> parseZlib

parseHeader :: Parser Bool
parseHeader = switch (long "has-header" <>  short 'h' <> help "Whether stream has a header")

parserOptions :: Parser Options
parserOptions = Options
  <$> parseCompression
  <*> parseHeader
  <*> parseFormat
  <*> (option auto (long "parallelism" <> short 'p' <> help "Maximum number of threads to use." <> value 1))

parseArgs :: Parser Args
parseArgs = Args
  <$> (argument str (metavar "COMMAND"))
  <*> parserOptions

parseOpts = info (parseArgs <**> helper) 
              (fullDesc <> progDesc "QQ - The quicker tool for querying text data." )
