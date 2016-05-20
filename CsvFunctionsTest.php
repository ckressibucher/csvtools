<?php

namespace Ckr\CSV\Test;

use Ckr\CSV;
use PHPUnit_Framework_TestCase;
use RuntimeException;

class CsvFunctionsTest extends PHPUnit_Framework_TestCase
{

    /**
     * @test
     */
    public function its_toAssoc_maps_header_row_to_all_following_rows()
    {
        $input = [
            ['a', 'b', 'c'],
            [1, 2, 3],
            [11, 22, 33],
        ];
        $expected = [
            ['a' => 1, 'b' => 2, 'c' => 3],
            ['a' => 11, 'b' => 22, 'c' => 33],
        ];
        $output = CSV\toAssoc($this->toIterable($input));
        $this->assertEquals($expected, iterator_to_array($output));
    }

    /**
     * @test
     */
    public function its_toAssoc_throws_error_if_not_same_length()
    {
        $input = [
            ['a', 'b', 'c'],
            [1, 2], // only two cols
        ];
        $this->expectException(RuntimeException::class);
        $out = CSV\toAssoc($this->toIterable($input));
        iterator_to_array($out); //  execute generator
    }

    /**
     * @test
     */
    public function its_doSelect_selects_columns_by_key()
    {
        $input = [['a' => 1, 'b' => 20, 'c' => 2]];
        $expected = [['a' => 1, 'b' => 20]];
        $out = CSV\doSelect($this->toIterable($input), ['a', 'b']);
        $this->assertEquals($expected, iterator_to_array($out));
    }

    /**
     * @test
     */
    public function its_doFilter_filters_data_rows()
    {
        $input = [
            ['condition' => true, 'val' => 2],
            ['condition' => false, 'val' => 3],
        ];
        $expected = [
            ['condition' => true, 'val' => 2],
        ];
        $filter = function ($row) {
            return $row['condition'];
        };
        $out = CSV\doFilter($this->toIterable($input), $filter);
        $this->assertEquals($expected, iterator_to_array($out));
    }

    /**
     * @test
     */
    public function its_doMap_maps_rows()
    {
        $input = [
            'abc',
            'yyy',
        ];
        $expected = [
            'cba',
            'yyy',
        ];
        $out = CSV\doMap($this->toIterable($input), 'strrev');
        $this->assertEquals($expected, iterator_to_array($out));
    }

    /**
     * @test
     */
    public function its_doMap_returns_empty_generator_on_empty_input()
    {
        $input = new \ArrayObject();
        $out = CSV\doMap($input, 'strrev');
        $this->assertInstanceOf(\Generator::class, $out);
        $this->assertSame([], iterator_to_array($out));
    }

    /**
     * @test
     */
    public function its_buildFilter_creates_filterable_closure_yielding_a_generator()
    {
        $input = [['a' => 1]];
        $expected = [];

        $filter = function ($row) {
            return false; // dummy
        };
        // this should now be a callable which transforms a Traversable to a Generator
        $filterCallback = CSV\buildFilter($filter);
        $this->assertTrue(is_callable($filterCallback));

        $out = $filterCallback($this->toIterable($input));
        $this->assertInstanceOf(\Generator::class, $out);
        $this->assertSame($expected, iterator_to_array($out));
    }

    /**
     * @test
     */
    public function its_buildSelect_creates_a_select_closure_yielding_a_generator()
    {
        $input = [['a' => 1, 'b' => 2]];
        $fields = ['a'];
        $expected = [['a' => 1]];

        $selectable = CSV\buildSelect($fields);
        $this->assertTrue(is_callable($selectable));

        $out = $selectable($this->toIterable($input));
        $this->assertInstanceOf(\Generator::class, $out);
        $this->assertEquals($expected, iterator_to_array($out));
    }

    /**
     * @test
     */
    public function its_combine_stages_combines_processor_functions()
    {
        $input = [
            ['a' => 11, 'b' => 22],
            [],
        ];
        $expected = [ // filter and convert to string
            ['a' => '11', 'b' => '22'],
        ];
        $filter = function (\Traversable $data) {
            foreach ($data as $row) {
                if (!empty($row)) {
                    yield $row;
                }
            }
        };
        $toString = function (\Traversable $data) {
            foreach ($data as $row) {
                yield array_map('strval', $row);
            }
        };
        $pipeline = CSV\combineStages($filter, $toString);
        $this->assertTrue(is_callable($pipeline));

        $out = $pipeline($this->toIterable($input));
        $this->assertInstanceOf(\Generator::class, $out);
        $this->assertEquals($expected, iterator_to_array($out));
    }

    /**
     * @test
     */
    public function its_combine_stages_does_not_alter_input_with_identity_processor()
    {
        $input = [
            ['a' => 1],
        ];
        // this does nothing else than "converting" the Traversable to a Generator
        $idProc = function (\Traversable $data) {
            foreach ($data as $d) {
                yield $d;
            }
        };
        $pipeline = CSV\combineStages($idProc);
        $out = $pipeline($this->toIterable($input));
        $this->assertEquals($input, iterator_to_array($out));
    }

    /**
     * @test
     */
    public function its_combine_stages_does_not_alter_input_without_arguments()
    {
        $input = [
            ['a' => 1],
        ];
        $pipeline = CSV\combineStages();
        $out = $pipeline($this->toIterable($input));
        $this->assertInstanceOf(\Generator::class, $out);
        $this->assertEquals($input, iterator_to_array($out));
    }

    /**
     * @test
     */
    public function its_doCount_counts_traversable()
    {
        $input = $this->toIterable(['a', 'b']);
        $this->assertSame(2, CSV\doCount($input));
    }

    /**
     * @test
     */
    public function its_toPrint_prints_to_stdout()
    {
        ob_start();
        $input = $this->toIterable(['some string']);
        CSV\doPrint($input);
        $output = ob_get_clean();
        $this->assertContains('some string', $output);
    }

    /**
     * @test
     */
    public function its_readFromResource_returns_csv_row_generator()
    {
        $input = 'a,b,c' . PHP_EOL . '1,2,3';
        $inStream = fopen('php://memory', 'w+');
        fwrite($inStream, $input);
        rewind($inStream);

        $out = CSV\readFromResource($inStream);
        $this->assertInstanceOf(\Generator::class, $out);
        $outputArr = iterator_to_array($out);
        $this->assertEquals([['a', 'b', 'c'], ['1', '2', '3']], $outputArr);

        fclose($inStream);
    }

    /**
     * @test
     */
    public function its_writeToResource_writes_csv_rows()
    {
        $input = [
            ['a', 'b', 'c'],
            [1, 2, 3],
        ];
        $expectedOutput = ['a,b,c', '1,2,3'];
        $outStream = fopen('php://memory', 'w+');
        CSV\writeToResource($outStream, $this->toIterable($input));

        rewind($outStream);
        $line1 = fgets($outStream);
        $line2 = fgets($outStream);
        $this->assertSame($expectedOutput[0], trim($line1));
        $this->assertSame($expectedOutput[1], trim($line2));

        fclose($outStream);
    }

    private function toIterable(array $arr)
    {
        return new \ArrayObject($arr);
    }
}
