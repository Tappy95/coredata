import argparse
import importlib

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Set worker to run')
    parser.add_argument('-w', '--worker', dest='worker', required=True,
                        help='worker name')
    args = parser.parse_args()
    if args.worker == 'amazon_keyword':
        worker = importlib.import_module('.worker', 'amazon_keyword')
        worker.run()
    elif args.worker == 'amazon_category':
        worker = importlib.import_module('.worker', 'amazon_category')
        worker.run()
    elif args.worker == 'amazon_product':
        worker = importlib.import_module('.worker', 'amazon_product')
        worker.run()
    elif args.worker == 'amazon_product_relationship':
        worker = importlib.import_module('.relationship_worker', 'amazon_product')
        worker.run()
    else:
        raise ValueError('not support worker')
