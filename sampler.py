import platform, logging, os
import pandas as pd
import pytz

#########Logging configuration##########
if platform.platform().startswith('Windows'):
    logging_file = os.path.join(os.getcwd(), os.path.splitext(os.path.basename(__file__))[0]+'.log')
else:
    logging_file = os.path.join(os.getcwd(), os.path.splitext(os.path.basename(__file__))[0]+'.log')

log_formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(threadName)-9s : %(message)s')
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)

fileHandler = logging.FileHandler(logging_file)
fileHandler.setFormatter(log_formatter)
fileHandler.setLevel(logging.DEBUG)
LOGGER.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(log_formatter)
consoleHandler.setLevel(logging.DEBUG)
LOGGER.addHandler(consoleHandler)
#########Logging configuration ends##########
__version__ = "2.0"


class TDResampler:
    """
    class representing each Trading Data Sample object to be resampled.
    It takes csv file as input & can perform various resampling operations.
    """
    def __init__(self, csv_file, datetimecols=['Date', 'Time'], cols=["Date","Time","Open","High","Low","Close","Up","Down"]):
        """
        Initialize the object
        :param csv_file: path of csv file
        :param datetimecols: provide a list of names of date & time columns in the csv file
        :param cols: provide a list of csv file columns
        """
        self.csv_file = csv_file
        self.datetime_col = datetimecols
        self.cols = cols

        # check csv file & exit if not found.
        self.__check_file(True)

        # read file in pandas dataframe
        self.df_orig = pd.read_csv(csv_file,
                              parse_dates={'DateTime': datetimecols},
                              usecols=cols,
                              infer_datetime_format=True,
                              na_values=['nan']).set_index('DateTime')
        self.output_header = True
        self.ohlc_dict = None
        self.df = self.df_orig.copy()
        self.tz = None

    def __check_file(self, on_exit=False):
        """
        :param on_exit: exit on failure or not
        """
        if not os.path.isfile(self.csv_file):
            if on_exit:
                LOGGER.error("{} isn't a file. Exiting.".format(self.csv_file))
                exit(1)
            LOGGER.warning("{} isn't a file.".format(self.csv_file))

    def apply_cutoff(self, start_time=-1, end_time=-1, start_date=-1, end_date=-1):
        """
        Apply cutoff date or/and time to handle exceptions to human calendar and
        time of day cut-off points where the resampling interval does not fit
        perfectly in the final interval in a resampling period.
        :param start_time: Cut off dataframe time series before this time during any day
        :param end_time: Cut off dataframe time series after this time during any day
        :param start_date: Cut off dataframe time series before this date
        :param end_date: Cut off dataframe time series after this date
        """
        if start_time != -1 and end_time != -1:
            LOGGER.debug("Applying cutoff for start_time={} & end_time={}".format(start_time, end_time))
            self.df = self.df.between_time(start_time, end_time)

        if start_date != -1:
            LOGGER.debug("Applying cutoff for start_date={}".format(start_date))
            self.df = self.df.query("index > '{}'".format(start_date))

        if end_date != -1:
            LOGGER.debug("Applying cutoff for end_date={}".format(end_date))
            self.df = self.df.query("index < '{}'".format(end_date))

    def convert_tz(self, from_tz, to_tz):
        """
        Convert timezone of dataframe
        :param from_tz: Dataframe data current timezone
        :param to_tz: Dataframe  data target timezone
        """
        LOGGER.debug("Converting timezone from {} to {}".format(from_tz, to_tz))
        try:
            self.df.index = self.df.index.tz_localize(pytz.timezone(from_tz)).tz_convert(pytz.timezone(to_tz))
            self.tz = to_tz
        except pytz.exceptions.UnknownTimeZoneError:
            LOGGER.error("UnknownTimeZoneError")
        except:
            LOGGER.error("Some exception")

    def apply_precision(self, precision=2):
        """
        Apply precision to dataframe
        :param precision: Number of decimal places to round each column to.
        """
        if self.df is not None:
            LOGGER.debug("Applying Precision to {} decimal places".format(precision))
            self.df = self.df.round(decimals=int(precision))

    def resample(self, interval, ohlc_dict, param_dict):
        """
        Resample ("downsample") the dataframe and create weekly, bi-monthly,
        monthly, bi-quarterly, quarterly, annual, etc. data files with
        different "compression" and "frequencies" of the original data
        :param interval: The offset string or object representing target conversion; e.g: W, M, S
        :param ohlc_dict: OHLC dict
         for example:
         OHLC_DICT = {
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Up': 'last',
            'Down': 'last'
            }
        :param param_dict: Resample dict
            for example:
            RESAMPLE = {
                'closed': 'left',
                'label' : 'right,
                }
        """
        self.ohlc_dict = ohlc_dict
        LOGGER.debug("Calling Resample with arguments: {} \nand OHLC_DICT: {}".format(param_dict, ohlc_dict))
        if interval == 'W':
            self.df = self.df.resample('W-FRI', **param_dict).agg(ohlc_dict).dropna(how='any')
        else:
            self.df = self.df.resample(interval, **param_dict).agg(ohlc_dict).dropna(how='any')
        return self.df

    def write_csv(self, outfile, columns=["Open","High","Low","Close","Up","Down"]):
        """
        Write the dataframe to csv file.
        Any col not present in ohlc_dict is discarded
        :param outfile: output csv file name with path
        :param cols: output csv file name headers
        """
        for val in self.datetime_col:
            if val in columns:
                columns.remove(val)
        # discard any col not in ohlc_dict
        keys = self.ohlc_dict.keys()
        for val in columns:
            if val not in keys:
                LOGGER.warning("col {} not in resampled data. discarded.".format(val))
                columns.remove(val)
        self.df = self.df[columns]

        LOGGER.debug("Writing to disk with following columns: {}".format(columns))
        self.df.to_csv(outfile, header=self.output_header)




def check_file(file, on_exit=False):
    if not os.path.isfile(file):
        if on_exit:
            LOGGER.error("{} isn't a file. Exiting.".format(file))
            exit(1)
        LOGGER.warning("{} isn't a file.".format(file))


if __name__ == "__main__":
    """
    Execution starts here.
    """

    tdsample = TDResampler('input.csv')
    tdsample.convert_tz('US/Central', 'US/Eastern')
    ohlc_dict = {
        'Open':'first',
        'High': 'max',
        'Low': 'min',
        'Close' : 'last',
        'Up' : 'last',
        'Down': 'last'
    }
    RESAMPLE = {
        'closed': 'left',
        'label': 'right'
    }
    tdsample.resample('D', ohlc_dict, RESAMPLE)
    tdsample.apply_cutoff(start_date='12/20/2018', end_date='1/08/2019')
    tdsample.apply_precision()
    tdsample.write_csv('out.csv')





    
