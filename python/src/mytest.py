#!/usr/bin/env python
# coding=utf-8
import unittest
from .tests.tiers.test_queue import TestJob

suite = unittest.TestLoader().loadTestsFromTestCase(TestJob)
unittest.TextTestRunner(verbosity=3).run(suite)
