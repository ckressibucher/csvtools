# Process CSV datasets efficiently

[![Build Status](https://travis-ci.org/ckressibucher/csvtools.svg?branch=master)](https://travis-ci.org/ckressibucher/csvtools)
[![Coverage Status](https://coveralls.io/repos/github/ckressibucher/csvtools/badge.svg?branch=master)](https://coveralls.io/github/ckressibucher/csvtools?branch=master)

This is just a collection of functions to process
CSV (and other) data structures. The functions are
designed in such a way that they can be combined
to build processing pipelines. All functions return
`Generator`s to ensure that big data sets can be
processed without having everything in memory.

The concepts are inspired by lazy functional
programming which is very cool :-)

Example (from example.php)
---------------------------

```php
// don't worry, there is also a composer autoloader
// defined
include './csv_functions.php';

use Ckr\CSV as C;

// First, we want to filter the products,
// so we only have the healthy ones...
$filterHealthy = C\buildFilter(
    function (array $row) {
        return $row['is_healthy'] > 5;
    }
);

// We're not interested in the inventory, so
// we select only the other fields
$selectFields = C\buildSelect(
    ['id', 'product', 'price']
);

// Unfortunately, this are not the real prices,
// we also have to add taxes...
$mapAddTaxes = C\buildMap(
    function (array $row) {
        $priceWithTax = floatval($row['price']) * 1.20;
        $row['price_incl_tax'] = $priceWithTax;
        return $row;
    }
);

// Now, lets combine this processing stages to
// a pipeline
$pipeline = C\combineStages(
    $filterHealthy,
    $selectFields,
    $mapAddTaxes
);

// Ehm..., and we also need to have nice string representation
// of the products.
// No problem, we can combine the
// current pipeline -- which is itself a processing stage --
// with another processor
$mapStringField = C\buildMap(
    function (array $row) {
        $str = sprintf(
            '%s (%d) is healthy product and costs only $%s',
            $row['product'],
            intval($row['id']),
            number_format($row['price_incl_tax'], 2)
        );
        $row['as_text'] = $str;
        return $row;
    }
);

// build the new pipeline. Note, that we also need to
// map the "primitive" indexed arrays to associative
// arrays. This is done by the 'toAssoc' function
$newPipeline = C\combineStages(
    'Ckr\\CSV\\toAssoc',
    $pipeline,
    $mapStringField
);

// Now we want to read the csv file, process each row
// and write the result to the output
$input = './example.csv';

// Note that this is still a generator, not the actual
// data. The input file has not yet been read!
$data = $newPipeline(C\readFromFile($input));

// Then we write everything to stdout
$outStream = fopen('php://stdout', 'wb');
C\writeToResource($data, $outStream);
```
