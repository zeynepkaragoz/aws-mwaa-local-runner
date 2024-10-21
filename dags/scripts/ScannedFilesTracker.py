import pandas as pd


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        else:
            cls._instances[cls].__init__(*args, **kwargs)
        return cls._instances[cls]


class ScannedFilesTracker(metaclass=Singleton):

    def __init__(self, filename, *args, **kwargs):

        self.file_list = []
        self.dates_list = []
        self.dataframe = self.read_tracker(filename)

    def read_tracker(self, filename):
        # Finds modified folders (i.e. new files in them), saves .csv files that were added
        try:
            previously_scanned_files = pd.read_csv(filename, sep='\t', comment='#')
        except:
            previously_scanned_files = pd.DataFrame(columns=['Filename', 'Datetime'])
            previously_scanned_files.to_csv(filename, sep='\t', index=False)
        return previously_scanned_files

    def add_scanned_file(self, filename, datetime, *args, **kwargs):
        self.file_list.append(filename)
        self.dates_list.append(datetime)

    def to_dataframe(self):
        df = pd.DataFrame.from_dict({'Filename': self.file_list, 'Datetime': self.dates_list})
        return df

    def has_file(self, filename, datetime):
        has_new_file = not self.dataframe.loc[
            (self.dataframe['Filename'] == filename) &
            (self.dataframe['Datetime'] == datetime)].empty
        return has_new_file

    def save_and_close(self, files, SCANNED_FILES_RECORD):
        df_currently_read_files = pd.DataFrame.from_dict(files, orient='index')
        df_currently_read_files = df_currently_read_files.reset_index()
        df_currently_read_files.columns = ['Filename', 'Datetime']
        previously_scanned_files = pd.concat([self.dataframe, df_currently_read_files], axis=0, ignore_index=True)
        previously_scanned_files.to_csv(SCANNED_FILES_RECORD, sep='\t', index=False)




