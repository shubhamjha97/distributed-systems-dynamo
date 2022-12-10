#!/bin/bash

python test_dynamo.py --seed 10 SimpleTestCase.test_simple_put > test1.out
python test_dynamo.py --seed 10 SimpleTestCase.test_get_put_get_put > test2.out
python test_dynamo.py --seed 10 SimpleTestCase.test_partition > test3.out
