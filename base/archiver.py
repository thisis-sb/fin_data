import os
import gzip
import pickle
from global_env import LOG_DIR

class Archiver:
    archive_name = None
    xbrl_dict = None
    mode = None
    active = None
    compressed = None

    def __init__(self, full_path, mode, compressed=True, overwrite=False):
        assert mode == 'r' or mode == 'w'

        if mode == 'w':
            if os.path.exists(full_path):
                if overwrite:
                    os.remove(full_path)
                else:
                    raise RuntimeError(f'archive {full_path} already exists')
            self.xbrl_dict = {}
        else:
            if not os.path.exists(full_path):
                raise RuntimeError(f'archive {full_path} does not exist')
            if compressed:
                with gzip.open(full_path, 'rb') as f:
                    self.xbrl_dict = pickle.load(f)
            else:
                with open(full_path, 'rb') as f:
                    self.xbrl_dict = pickle.load(f)

        self.compressed = compressed
        self.archive_name = full_path
        self.mode = mode
        self.active = True

    def add(self, xbrl_name, xbrl_data):
        if self.mode == 'w' and self.active:
            self.xbrl_dict[xbrl_name] = xbrl_data

    def get(self, xbrl_name):
        return self.xbrl_dict[xbrl_name] if (self.mode == 'r' and self.active) else None

    def size(self):
        return len(self.xbrl_dict) if self.active else None

    def flush(self):
        if self.active and self.mode == 'w':
            if self.compressed:
                with gzip.open(self.archive_name, 'wb') as f:
                    pickle.dump(self.xbrl_dict, f)
            else:
                with open(self.archive_name, 'wb') as f:
                    pickle.dump(self.xbrl_dict, f)
            self.active = None
            self.archive_name = None
            self.xbrl_dict = None
            self.mode = None
            self.compressed = None

# temporary for legacy ------------------------
STORED_AS_CSV = False

def df_to_file(df, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    if not STORED_AS_CSV:
        df.to_parquet(filename + '.parquet', index=False, engine='pyarrow', compression='gzip')
    else:
        df.to_csv(filename, index=False)
    return

# temporary for legacy ------------------------

# --------------------------------------------------------------------------------------------
if __name__ == '__main__':
    archive_name = LOG_DIR + '/test_archive'
    ARCHIVE_SIZE = 1000000

    a1 = Archiver(archive_name, 'w', overwrite=True, compressed=True)
    for i in range(0, ARCHIVE_SIZE):
        a1.add(f'id_{i}', 100*f'{i}')
    a1.flush()
    print('Test 1: OK')

    try:
        a2 = Archiver(archive_name, 'w')
        print('THIS SHOULD NEVER BE PRINTED')
    except:
        print('Test 2: OK')

    a3 = Archiver(archive_name, 'r', compressed=True)
    assert a3.size() == ARCHIVE_SIZE, 'Test 3 failed'
    print('Test 3: OK')

    for i in range(1, ARCHIVE_SIZE):
        assert a3.get(f'id_{i}') == 100*f'{i}', 'Test 4 failed'

    print('Test 4: OK')

    try:
        a1 = Archiver(archive_name, 'w')
    except:
        print('Test 5: OK')

    print('All tests passed')