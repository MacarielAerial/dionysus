#!/bin/bash -e

echo "Testing..."
coverage run -m pytest src/tests && coverage report -m
