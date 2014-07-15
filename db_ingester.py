"""A module providing the DBIngester class for processing data files and
incorporating them into an sqlite database for further analysis."""
import sqlite3, os, re
import warnings
from pathlib import Path

class DBIngester(object):
    """Class providing methods for organizing and formatting data for input
    into an sqlite database."""
    connection = None
    cursor = None
    file_subtypes = []

    def __init__(self, file_name, prompt_for_input=False, table_name='entries',
                 row_spec=''):
        self.cursor = None
        if os.path.isfile(file_name):
            self.connect_db(file_name)
        else:
            self.create_db(file_name, prompt_for_input, table_name, row_spec)

    def create_db(self, file_name, prompt_for_input=False, table_name='entries',
                  row_spec=''):
        """Create a sqlite database"""
        if not os.path.splitext(file_name)[1]:
            file_name = file_name + '.db'

        self.connect_db(file_name)

        if prompt_for_input:
            table_name = shortcut_prompt('Enter database table name',
                                         default=table_name)
            row_spec = shortcut_prompt("""
            Enter table row specification.
            Format is comma separated list of column names and data types.
            Example: date text, name text, qty real, price real
            Possible data types: NULL, INTEGER, REAL, TEXT, BLOB
            """)

        if table_name and row_spec:
            self.create_table(table_name, row_spec)

    def connect_db(self, file_name):
        """Connect to sqlite .db file"""
        self.connection = sqlite3.connect(file_name)
        self.cursor = self.connection.cursor()

    def create_table(self, table_name, row_spec):
        """Create a new table in the database"""
        self.cursor.execute('CREATE TABLE ? (?)', table_name, row_spec)

    def process_file(self, file_path):
        """Read in data from file and add it to database"""
        file_subtype = self.determine_file_subtype(file_path)
        # MORE

    def determine_file_subtype(self, file_path):
        """Determine parsable file subtype from file_path"""
        return file_path


class FileSubtype(object):
    """
    Object containing all information for parsing files of particular subtype
    """
    def __init__(self, subtype_name, file_id_rules, file_subtype,
                 file_format='csv'):
        # file_format vs. file_subtype:
        # format specifies the base formatting of the file (e.g. CSV or
        # plain text).
        # subtype specifies the particular subtype of file in that format
        # (e.g. Company X CSV spreadsheet vs. Company Y CSV spreadsheet).
        self.name = subtype_name
        self.file_id_rules = file_id_rules
        self.file_format = file_format


    def check_path(self, path):
        """
        Check that path either matches the name or path rule for the file
        subtype (returns boolean)

        Special path strings:
        !ROOT!: Goes at the beginning of the path rule. Denotes that the path
            is absolute.
        !DIRS!: Denotes that zero or more intermediate directories go between
            the preceding and the next directory rule in the path string
        !DIRS_N!: Denotes that exactly N directories go between the preceding
            and the next directory rule in the path string

        Note: '/' character not allowed in directory names
        """
        path_match = PathMatch(self.file_id_rules)
        return path_match.check_path(path)


class PathMatch(object):
    """
    Class for matching a Path object against a set of rules
    """
    def __init__(self, rules_dict):
        self.rules_dict = rules_dict

        self.patterns = None
        self.path_parts = None
        self.pattern = None
        self.part = None
        self.match_state = 'undetermined'
        self.variable_skip = False


    def check_path(self, path):
        """
        Check that path either matches the name or path rule for the file
        subtype (returns boolean)

        Special path strings:
        !ROOT!: Goes at the beginning of the path rule. Denotes that the path
            is absolute.
        !DIRS!: Denotes that zero or more intermediate directories go between
            the preceding and the next directory rule in the path string
        !DIRS_N!: Denotes that exactly N directories go between the preceding
            and the next directory rule in the path string

        Note: '/' character not allowed in directory names
        """
        name_match = self.check_name(path)
        dirs_match = self.check_directories(path)

        # TODO: allow name_match or dirs_match
        return name_match and dirs_match


    def check_name(self, path):
        """
        Check if file name of path matches name_rule expression
        """
        if 'name' in self.rules_dict:
            name_rule = self.rules_dict['name']
        else:
            name_rule = False

        if name_rule and re.match(name_rule, path.name):
            match_bool = True
        else:
            match_bool = False

        return match_bool


    def initialize_check_dirs_loop(self, path):
        """
        Set up the patterns and path_parts iterables for comparing path to the
        directories rule.  Initialize pattern and part to the first element of
        each, respetively.  patterns refers to the path-like set of regular
        expressions described in check_path.  path_parts refers to a path (not
        including the file name, only the file's parent directory) broken up
        into individual directories.
        """
        dirs_rule = self.rules_dict['directories']

        # Prepare iterables
        self.patterns = self.prepare_path_patterns(dirs_rule)
        self.path_parts = self.prepare_path_parts(path)

        # Get first elements for matching
        self.pattern = next(self.patterns, None)
        self.part = next(self.path_parts, None)


    def check_directories(self, path):
        """
        Loop through directories in path to see if they match the rules
        specified in rules_dict['directories']
        """
        # Method technical description:
        # This method started out as a single function, but it required
        # numerous branches and was messy. It was broken out into several
        # subfunctions, but many variables needed to be passed back and forth
        # between them and the result was still messy. So the function was
        # incorporated into the PathMatch class which allowed all of the
        # variables being passed back and forth to become class properties that
        # were checked and set within the subfunctions. Now there is still the
        # danger of confusion due to properties being changed within
        # subfunctions which is not ideal for clarity.
        #
        # Here are the class properties being handled in this method:
        # patterns: iterator version of all of the patterns making up the
        # directory structure to be matched (created from
        # rules_dict['directories']
        # path_parts: iterator version of the path to check against patterns
        # for a match
        # pattern: the current pattern taken from patterns to be checked
        # against the current path part
        # part: the current path part taken from path_parts
        # match_state: string with value 'undetermined', 'failed', or
        # 'succeeded' depending on whether it has been determined or not if
        # path matches patterns.
        # variable_skip: boolean setting whether or not match_state is set to
        # failed when part does not match pattern (otherwise part is iterated
        # without failure)
        #
        # Within the main while loop, generally, pattern and part are drawn
        # from patterns and path_parts and compared for a match. If they match,
        # the next pattern and part are drawn; otherwise, the match is
        # considered a failure. For special patterns !DIRS_N! or !DIRS!,
        # pattern and part are not iterated in sync with each other in order to
        # match the speical behavior of these patterns. The special pattern
        # !ROOT! is also handled differently.
        if 'directories' in self.rules_dict:
            # Prepare iterables
            self.initialize_check_dirs_loop(path)
            self.match_state = 'undetermined'
            self.variable_skip = False
        else:
            self.match_state = 'failed'
            return False

        while self.match_state is 'undetermined':
            (rule_type, rule_data) = self.get_rule_type(self.pattern)

            if rule_type is 'root':
                self.eval_rule_root()
            elif rule_type is 'skip_fixed':
                self.eval_rule_skip_fixed(rule_data)
            elif rule_type is 'skip_variable':
                self.eval_rule_skip_variable()
            elif rule_type is 'dir_name':
                self.eval_rule_dir_name()

            self.validate_pattern_part()

        if self.match_state is 'succeeded':
            return True
        else:
            return False


    @staticmethod
    def get_rule_type(pattern):
        """
        Determine which type of file path matching rule type pattern is.
        """
        rule_data = None
        if pattern is '!ROOT!':
            rule_type = 'root'
        if pattern is '!DIRS!':
            rule_type = 'skip_variable'
        else:
            match_dirs_n = re.match(r'!DIRS_(?P<number>\d+)!', pattern)
            if match_dirs_n:
                rule_type = 'skip_fixed'
                rule_data = int(match_dirs_n.group('number'))
            else:
                rule_type = 'dir_name'

        return (rule_type, rule_data)

    @staticmethod
    def prepare_path_patterns(path_pattern_string):
        """
        Convert path_pattern_string into iterable for looping through the
        patterns on a given path.
        """
        patterns = path_pattern_string.split(sep='/')
        patterns.reverse()
        patterns = iter(patterns)
        return patterns


    @staticmethod
    def prepare_path_parts(path):
        """
        Convert Path object path into an iterable of the path directories in
        reverse order.
        """
        path_parts = list(path.parent.absolute())
        path_parts.reverse()
        path_parts = iter(path_parts)
        return path_parts


    def eval_rule_root(self):
        """
        Check if path_parts satisfies the root rule: the most recent path part
        extracted from path_parts was the last one. The one exception is when
        the rule is !ROOT!/!DIRS!, but note that this is equivalent to !DIRS!)
        """
        if next(self.path_parts, None) is None:
            self.match_state = 'succeeded'
        elif self.variable_skip:
            self.match_state = 'succeeded'
            warnings.warn('!ROOT! is unnecessary when followed by !DIRS!')
        else:
            self.match_state = 'failed'


    def eval_rule_skip_fixed(self, skip_number):
        """
        Skip the next skip_number-1 elements of path_parts. If path_parts does
        not have this many elements, set match_state to failed. Then return the
        next elements of path_parts and patterns as part and pattern.
        """
        for _ in range(skip_number-1):
            next(self.path_parts, None)
        if self.part is None:
            self.match_state = 'failed'

        self.part = next(self.path_parts, None)
        self.pattern = next(self.patterns, None)


    def eval_rule_skip_variable(self):
        """
        Process the variable skip rule: Extract the next pattern and set
        variable_skip to True
        """
        self.pattern = next(self.patterns, None)
        self.variable_skip = True


    def eval_rule_dir_name(self):
        """
        Process dir name rule: Check if pattern and part match. If they do,
        extract the next pattern and part and set variable_skip to false. If
        they do not and variable_skip is true, move on to the next part.
        Otherwise, set match_state to failed.
        """
        if re.match(self.pattern, self.part):
            self.pattern = next(self.patterns, None)
            self.part = next(self.path_parts, None)
            self.variable_skip = False
        elif self.variable_skip:
            self.part = next(self.path_parts, None)
        else:
            self.match_state = 'failed'

    def validate_pattern_part(self):
        """
        Check if either patterns or parts have been exhausted
        """
        if self.pattern is None:
            self.match_state = 'succeeded'
        elif self.part is None:
            self.match_state = 'failed'

class CSVSubtype(FileSubtype):
    """
    Object containing all information for parsing csv files of various file
    subtypes.
    """
    def __init__(self, subtype_name, file_id_rules, format_spec):
        FileSubtype.__init__(self, subtype_name, file_id_rules,
                             file_format='csv')
        self.first_row = format_spec['first_row']
        self.columns = format_spec['columns']


def file_subtype_factory(file_subtype_spec):
    """
    Factory function for producing the correct subclass of FileSubtype.
    """
    if file_subtype_spec['file_format'] is 'csv':
        file_subtype = CSVSubtype(file_subtype_spec)
    else:
        file_subtype = None

    return file_subtype


def shortcut_prompt(text, default='', shortcuts=''):
    """Prompt for input with default response and shortcuts"""
    print(text)
    if shortcuts:
        print('Shortcuts: {}'.format(shortcuts))
    if default:
        print('Default: {}'.format(default))
    response = input('>>> ')
    if not response:
        response = default

    if shortcuts and response in shortcuts:
        response = shortcuts[response]

    return response

# testdd
