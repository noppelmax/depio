# File: test_Task__get_not_updated_products.py

from depio.Task import _get_not_updated_products


def test_get_not_updated_products_no_updates():
    product_timestamps_before_running = {'product1': 'timestamp1', 'product2': 'timestamp2'}
    product_timestamps_after_running = {'product1': 'timestamp1', 'product2': 'timestamp2'}
    not_updated_products = _get_not_updated_products(product_timestamps_after_running, product_timestamps_before_running)
    assert not_updated_products == ['product1', 'product2']


def test_get_not_updated_products_some_updates():
    product_timestamps_before_running = {'product1': 'timestamp1', 'product2': 'timestamp2'}
    product_timestamps_after_running = {'product1': 'timestamp1', 'product2': 'new_timestamp2'}
    not_updated_products = _get_not_updated_products(product_timestamps_after_running, product_timestamps_before_running)
    assert not_updated_products == ['product1']


def test_get_not_updated_products_all_updates():
    product_timestamps_before_running = {'product1': 'timestamp1', 'product2': 'timestamp2'}
    product_timestamps_after_running = {'product1': 'new_timestamp1', 'product2': 'new_timestamp2'}
    not_updated_products = _get_not_updated_products(product_timestamps_after_running, product_timestamps_before_running)
    assert not_updated_products == []


def test_get_not_updated_products_different_order():
    product_timestamps_before_running = {'product1': 'timestamp1', 'product2': 'timestamp2'}
    product_timestamps_after_running = {'product2': 'timestamp2', 'product1': 'timestamp1'}
    not_updated_products = _get_not_updated_products(product_timestamps_after_running, product_timestamps_before_running)
    assert not_updated_products == ['product1', 'product2']

