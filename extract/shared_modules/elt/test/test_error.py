import shared_modules.elt.error
import logging


def do_raise(error_type, *args):
    raise error_type(*args)


def test_aggregate_default():
    aggregator = shared_modules.elt.error.ExceptionAggregator(Exception)

    for i in range(0, 10):
        aggregator.call(do_raise, Exception, "Error {}".format(i))

    try:
        aggregator.raise_aggregate()
    except Exception as e:
        logging.info(str(e))


def test_aggregate_custom():
    @shared_modules.elt.error.aggregate
    class CustomError(shared_modules.elt.error.Error):
        pass

    aggregator = shared_modules.elt.error.ExceptionAggregator(CustomError)

    for i in range(0, 10):
        aggregator.call(do_raise, CustomError, "Error {}".format(i))

    try:
        aggregator.raise_aggregate()
    except CustomError as custom:
        logging.info(str(custom))
    except shared_modules.elt.error.Error as err:
        raise "Catched by the shared_modules.elt.error.Error clause."
    except Exception as e:
        raise "Catched by the Exception clause."
