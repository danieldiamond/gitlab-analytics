import elt.error
import logging


def do_raise(error_type, *args):
    raise error_type(*args)


def test_aggregate_default():
    aggregator = extract.error.ExceptionAggregator(Exception)

    for i in range(0, 10):
        aggregator.call(do_raise, Exception, "Error {}".format(i))

    try:
        aggregator.raise_aggregate()
    except Exception as e:
        logging.info(str(e))


def test_aggregate_custom():
    @extract.error.aggregate
    class CustomError(extract.error.Error):
        pass

    aggregator = extract.error.ExceptionAggregator(CustomError)

    for i in range(0, 10):
        aggregator.call(do_raise, CustomError, "Error {}".format(i))

    try:
        aggregator.raise_aggregate()
    except CustomError as custom:
        logging.info(str(custom))
    except extract.error.Error as err:
        raise "Catched by the extract.error.Error clause."
    except Exception as e:
        raise "Catched by the Exception clause."
