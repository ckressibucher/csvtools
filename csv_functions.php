<?php

namespace Ckr\CSV;

use Generator;
use Traversable;

// =========================================================================
// == Data Sources                                                  ========
// =========================================================================

// Note: additionally to using this data sources, you can also directly
// provide datasets as any kind of `Traversable` data structure
// to most of the processing functions below.

/**
 * Reads a file line by line and parses it to CSV
 *
 * @param string $path      Filepath
 * @param string $delimiter CSV Delimiter used for parsing
 * @param string $enclosure CSV Enclosure used for parsing
 * @return Generator        Generator or arrays (one arr per line)
 */
function readFromFile(
    string $path,
    string $delimiter = ',',
    string $enclosure = '"'
): Generator {
    if (!file_exists($path)) {
        throw new \RuntimeException('file ' . $path . ' does not exist');
    }
    $fh = fopen($path, 'rb');
    foreach (readFromResource($fh, $delimiter, $enclosure) as $resultRow) {
        yield $resultRow;
    }
    fclose($fh);
}

/**
 * Reads CSV rows line by line from the given resource
 *
 * @param resource $inputStream      Resource to read from (Will not be closed by this function!)
 * @param string $delimiter CSV Delimiter used for parsing
 * @param string $enclosure CSV Enclosure used for parsing
 * @return Generator        Generator or arrays (one arr per line)
 */
function readFromResource(
    $inputStream,
    string $delimiter = ',',
    string $enclosure = '"'
): Generator {
    while (false !== ($line = fgetcsv($inputStream, null, $delimiter, $enclosure))) {
        if (null === $line) {
            throw new \RuntimeException('probably invalid resource');
        }
        if (1 === count($line) && null === $line[0]) {
            continue;
        }
        yield $line;
    }
}

// =========================================================================
// == Data Processing functions                                     ========
// =========================================================================

/**
 * Maps every row to an assoc array, where the values
 * of the first row as the keys.
 *
 * @param Traversable $data
 * @return Generator
 */
function toAssoc(Traversable $data): Generator
{
    $keys = null;
    foreach ($data as $row) {
        if (null === $keys) {
            // first row
            $keys = $row;
            continue;
        }
        if (count($row) !== count($keys)) {
            $msg = 'Error combining header keys with row. Probably not the same length';
            throw new \RuntimeException($msg);
        }
        $assoc = array_combine($keys, $row);
        if (!is_array($assoc)) {
            throw new \RuntimeException('something went wrong');
        }
        yield $assoc;
    }
}

/**
 * Selects fields from assoc dataset
 *
 * @see toAssoc to generate an assoc dataset
 *
 * @param Traversable $assocData
 * @param array $fields
 * @return Generator
 */
function doSelect(Traversable $assocData, array $fields): Generator
{
    foreach ($assocData as $row) {
        $newRow = [];
        foreach ($row as $k => $v) {
            if (in_array($k, $fields)) {
                $newRow[$k] = $v;
            }
        }
        yield $newRow;
    }
}

/**
 * @param Traversable $data
 * @param callable $filter Gets an row (of the $data) as input and is
 *                         expected to return a bool: true if row should
 *                         be kept, false if it should not be yielded for
 *                         the new generator
 * @return Generator
 */
function doFilter(Traversable $data, callable $filter): Generator
{
    foreach ($data as $row) {
        if (call_user_func($filter, $row)) {
            yield $row;
        }
    }
}

/**
 * Applies the mapper function to each data record
 *
 * @param Traversable $data
 * @param callable $mapper
 * @return Generator
 */
function doMap(Traversable $data, callable $mapper): Generator
{
    foreach ($data as $row) {
        yield $mapper($row);
    }
}

// =========================================================================
// === Helpers (builders, combinators)
// =========================================================================

/**
 * Builds a filter using `doFilter`
 *
 * @see doFilter
 *
 * This can be useful to build compositions (pipes, middlewares, ...)
 * of functions.
 *
 * @param callable $filter
 * @return \Closure
 */
function buildFilter(callable $filter): \Closure
{
    return function (Traversable $data) use ($filter) {
        return doFilter($data, $filter);
    };
}

/**
 * Builds a select function
 *
 * @param array $fields
 * @return \Closure
 */
function buildSelect(array $fields): \Closure
{
    return function (Traversable $data) use ($fields) {
        return doSelect($data, $fields);
    };
}

/**
 * Lets you combine different "stages" (functions) to
 * a pipeline of tasks which are executed sequentially
 * to process a given Traversable input set.
 *
 * All is done lazily. The output itself is a function
 * that maps a Generator to a new Generator, so it could
 * be used again as a stage for other combinations.
 *
 * @param \callable[] ...$stages Each stage is a callable function which maps a
 *                               Input Traversable to an output Generator
 * @return \Closure A closure which maps a Traversable input to a Generator
 */
function combineStages(callable ...$stages): \Closure
{
    return function (Traversable $in) use ($stages) {
        $data = $in;
        if (empty($stages) && !$data instanceof Generator) {
            // convert to generator (according to specs)
            $idProc = function (\Traversable $data) {
                foreach ($data as $d) {
                    yield $d;
                }
            };
            array_unshift($stages, $idProc);
        }
        foreach ($stages as $stage) {
            $data = call_user_func($stage, $data);
        }
        return $data;
    };
}

// =========================================================================
// === Data Sinks
// =========================================================================

/**
 * Writes the data given by the generator to the csv file
 *
 * @param Traversable $data
 * @param string $path
 * @param string $delimiter
 * @param string $enclosure
 * @param bool $overwrite
 */
function writeToFile(
    Traversable $data,
    string $path,
    string $delimiter = ',',
    string $enclosure = '"',
    $overwrite = true
) {

    if (file_exists($path)) {
        $ok = false;
        if ($overwrite) {
            $ok = unlink($path);
        }
        if (!$ok) {
            throw new \RuntimeException('there already is a file at ' . $path);
        }
    }
    $ok = touch($path);
    if (!$ok) {
        throw new \RuntimeException('Can not write to ' . $path);
    }
    $fh = fopen($path, 'wb');
    writeToResource($fh, $data, $delimiter, $enclosure);
    fclose($fh);
}

/**
 * Writes the data to a resource / stream
 *
 * @param resource $outputStream
 * @param Traversable $data
 * @param string $delimiter
 * @param string $enclosure
 */
function writeToResource(
    $outputStream,
    Traversable $data,
    string $delimiter = ',',
    string $enclosure = '"'
) {
    foreach ($data as $row) {
        $ok = fputcsv($outputStream, $row, $delimiter, $enclosure);
        if (false === $ok) {
            throw new \RuntimeException('error when writing resource');
        }
    }
}

/**
 * Traverses the data to count it
 *
 * @param Traversable $data
 * @return int
 */
function doCount(Traversable $data): int
{
    $cnt = 0;
    foreach ($data as $d) {
        $cnt++;
    }
    return $cnt;
}

/**
 * Prints each row
 *
 * @param Traversable $data
 */
function doPrint(Traversable $data)
{
    foreach ($data as $d) {
        print_r($d);
    }
}
