import re
from datetime import datetime
from bs4 import BeautifulSoup, NavigableString


def find_nb_shares(content, str_match, str_not_match, next_lines):
    """
       Parses and retrieves the number of shares located near a specified string (`str_match`) but before
       another string (`str_not_match`), considering different formatting conventions:

       - Pattern 1 corresponds to scenarios where the share count exceeds a thousand and is
         therefore written with a comma, enhancing readability for larger numbers.
       - Pattern 2 deals with share counts below a thousand, which are typically not comma-separated,
         addressing less common instances with precision.
       - `patterns_0` is a list capturing the various ways issuers might denote 'zero' in filings,
         ensuring even null values are accurately parsed.

       The function sequentially attempts to match Pattern 1, then Pattern 2, and finally `patterns_0`,
       minimizing mistakes by prioritizing more frequent formats. If no matches are found or `str_not_match`
       is encountered first, it defaults to returning zero, ensuring a fallback value is always provided.

       Args:
           content (str): The textual content within which to search for the number of shares. This should
                          be the full text from a document or filing.
           str_match (list of str): A list of strings or keywords to identify the starting point of the search.
                                    The function will look for share counts near these keywords.
           str_not_match (str): A string that, if encountered before finding a valid share count, causes the
                                function to return '0'. This is used to avoid false positives from unrelated
                                sections of the content.
           next_lines (int): The number of lines to consider after a `str_match` keyword has been found. This
                             allows the function to search within a specific range rather than the entire document.

       Returns:
           str: The number of shares found matching the specified criteria. Returns the number as a string,
                which may include comma separators for larger numbers. If no valid number is found, or if
                the `str_not_match` condition is met first, returns 'None'. Returns 'None' if the input content
                is `None`, indicating that no content was provided to search through.
       """
    if content is None:
        return None

    pattern2 = re.compile(r'\d{3}')
    pattern1 = re.compile(r'\b\d{1,3}(?:,\d{3})+\b')
    patterns_0 = [
        re.compile(r'\b0\b'),
        re.compile(r' 0 '),
        re.compile(r'-0-'),
        re.compile(r'None')
    ]

    lines = content.splitlines()
    for i, line in enumerate(lines):
        normalized_line = re.sub(r'\s+', ' ', line).lower()
        if any(match in normalized_line for match in str_match):
            for j in range(0, next_lines):
                if i + j < len(lines):
                    if str_not_match in lines[i + j].lower():
                        return 'None'
                    add1 = re.findall(pattern1, lines[i + j])
                    add2 = re.findall(pattern2, lines[i + j])
                    if add1:
                        shares_add = re.sub(r'[<> a-z]', '', add1[0])
                        return shares_add
                    if add2:
                        shares_add = re.sub(r'[<> a-z]', '', add2[0])
                        return shares_add
                    for pattern_0 in patterns_0:
                        add = re.findall(pattern_0, lines[i + j])
                        if add:
                            return '0'
    return 'None'


def find_percentage(content, str_match, str_not_match, next_lines):
    """
    Finds and returns a percentage value located near specified keywords (`str_match`) but before
    another keyword (`str_not_match`) in the provided content. This function considers various
    formatting conventions over a specified range of lines (`next_lines`) following a `str_match` occurrence.

    Patterns used:
    - `pattern1`: Identifies percentages with commas/decimals, followed by a '%' sign.
    - `pattern2`: Captures numeric values representing percentages without a '%' sign, covering a wide numeric range.
    - `pattern3`: Searches for numbers followed by an asterisk, indicating footnotes or special conditions.
    - `pattern4`: Finds floating-point numbers potentially representing percentages without a trailing '%'.

    Process:
    1. Searches for `str_match` and examines up to `next_lines` afterward, ignoring lines with `str_not_match`.
    2. Returns the first matched percentage as a string, removing non-numeric characters, except '.' and '-'.
    3. If 'see attachment' is found within the search range, returns 'None' for manual review.
    4. If no valid percentage is found after all patterns, aggregates `pattern2` matches and selects the last one,
       assuming it's the correct value if multiple numbers are detected.

    Defaults to '0' if no matches meet the criteria or if no percentage is detected.

    Args:
        content (str): Text content to be searched.
        str_match (list): Keywords indicating the start of the search section.
        str_not_match (str): A keyword that, if encountered, stops the search in the current range.
        next_lines (int): Number of lines to check following each `str_match` occurrence.

    Returns:
        str: The detected percentage matching criteria, 'None' for 'see attachment' and undetected %.
    """
    if content is None:
        return None

    pattern1 = re.compile(r'-?\d{1,3}(?:,\d{3})*(?:\.\d+)?%')
    pattern2 = re.compile(r'(?<=\s)(100(?:\.0{1,2})?|0(?:\.\d{1,2})?|[1-9]?\d(?:\.\d{1,2})?)')
    pattern3 = re.compile(r'-?\d+(?:\.\d+)?(?=[*])')
    pattern4 = re.compile(r'\b\d+\.\d+\b')
    lines = content.splitlines()
    matches = []
    for i, line in enumerate(lines):
        normalized_line = re.sub(r'\s+', ' ', line).lower()
        if any(match in normalized_line for match in str_match):
            for j in range(0, next_lines):
                if i + j < len(lines):
                    if str_not_match in lines[i +j].lower():
                        return 'None'
                    add_1 = re.findall(pattern1, lines[i + j])
                    add_3 = re.findall(pattern3, lines[i +j])
                    add_4 = re.findall(pattern4, lines[i + j])
                    if add_1:
                        shares_add = re.sub(r'[<> a-z%]', '', add_1[0])
                        return shares_add
                    if add_3:
                        shares_add = re.sub(r'[<> a-z%]', '', add_3[0])
                        return shares_add
                    if add_4:
                        shares_add = re.sub(r'[<> a-z%]', '', add_4[0])
                        return shares_add
                    if 'see attachment' in lines[i + j].lower():
                        return 'None'
                    add_2 = re.findall(pattern2, lines[i + j])
                    matches.extend(add_2)
            if matches:
                shares_add = re.sub(r'[<> a-z]', '', matches[-1])
                return shares_add
    return 'None'


def modify_text(element, limit=1000):
    """
        In some filings in html format, the text enclosed by a tag includes a newline character within the
        string I aim to match. This function recursively normalizes the whitespace in the text of a given element
        and its descendants within a hierarchical structure (such as HTML or XML) up to a specified limit of text
        modifications. It aims to ensure that there is exactly one space between words in the text, and no leading
        or trailing spaces, enhancing text consistency and readability.

        Parameters:
        - element: The root element from which to start modifying text. It is assumed that this element and its
          descendants can have text (NavigableString objects) and/or child elements.
        - limit (optional): The maximum number of text modifications to perform. Defaults to 1000. Some filings are
        really long, only the first NavigableString objects are necessary. Moreover, some very long filings also
        have some deeply nested structure at the end which triggers python's recursion limit. To limit the number
        of modifications at 1000 seems to avoid the issue.

        The function employs a nested helper function, _modify, which traverses the structure recursively,
        performing whitespace normalization on each text node encountered. The traversal and modifications
        halt when the limit is reached. A counter dictionary is used to track the number of modifications
        across the recursive calls.
        """
    counter = {'count': 0}

    def _modify(element):

        if counter['count'] >= limit:
            return
        if not element.contents:
            return
        for content in element.contents:
            if isinstance(content, NavigableString):
                new_text = ' '.join(content.strip().split())
                content.replace_with(new_text)
                counter['count'] += 1
            else:
                _modify(content)

            if counter['count'] >= limit:
                break
    _modify(element)


class FileSec:
    """
    The Filesec object is designed for parsing 13D/G filings, initialized with
    a specific file_path.

    - read_file_header: This method extracts key information from the highly
      structured headers of 13D/G filings, including the filing's issue date,
      company's name, owner's name, and both the company's and owner's CIKs.
      It reads the entire file to store header information in the header
      attribute, focusing solely on the file's header for information extraction,
      despite reading the full file. Another method, read_file_content, also reads
      the file but modifies it in ways that could complicate header parsing.

    - read_file_content: This method handles the parsing of 13D/G filings, which
      can be in text or HTML formats, by converting HTML formats into text format
      for uniform processing. It first uses the modify_text function to ensure
      that text within tags is newline-free. Then, it employs BeautifulSoup's
      get_text() method to extract text within tags. In cases where the text
      forms a large block, a newline is introduced for each tag listed in
      block_level_tags. This process populates the content attribute, which is
      subsequently used to extract important details such as the event date,
      number of shares, and the aggregate proportion of shares.
    """

    def __init__(self, file_path):
        self.file_path = file_path
        self.content = self.read_file_content()
        self.header = self.read_file_header()

    def read_file_header(self):
        try:
            with open(self.file_path, 'r') as file:
                content = file.read()
                return content
        except FileNotFoundError:
            print('File not found.')
            return None
        except IOError as e:
            print(f'Failed to open or read the file: {e}')
            return None

    def read_file_content(self):
        try:
            with open(self.file_path, 'r') as file:
                content = file.read()

                # Check if '<html>' is in the content (simple check for HTML presence)
                if '<html>' in content.lower():
                    # Parse the HTML content and extract text
                    soup = BeautifulSoup(content, 'lxml')
                    modify_text(soup)
                    text_only = soup.get_text(separator='\n')
                    return text_only
                else:
                    return content

        except FileNotFoundError:
            print('File not found.')
            return None
        except IOError as e:
            print(f'Failed to open or read the file: {e}')
            return None

    def find_file_type(self):
        if '13D' in self.file_path:
            return '13D'
        else:
            return '13G'

    def find_issue_date(self):
        if self.content is None:
            return None

        lines = self.header.splitlines()
        for line in lines:
            if 'FILED AS OF DATE' in line:
                date_str = re.sub('[^0-9]', '', line)
                date_add_issue = datetime.strptime(date_str, '%Y%m%d').strftime('%m-%d-%Y')
                return date_add_issue

    def find_transaction_date(self):
        if self.content is None:
            return 'None'

        # Updated pattern to match both written and numeric date formats, including 2-digit year
        date_pattern = (
            r'(January|February|March|April|May|June|'
            r'July|August|September|October|November|December)\s+\d{1,2}(?:\s*and\s*\d{1,2})?\s*,\s*\d{4}|'
            r'\d{1,2}/\d{1,2}/\d{2,4}'  # Keeps matching years with 2 or 4 digits
        )
        lines = self.content.splitlines()
        for i in range(len(lines)):
            str_match = ['date of event', 'effective date']
            if any(match in lines[i].lower() for match in str_match):
                # Concatenate the lines from 5 before to 5 after the current index for broader context
                concatenated = ' '.join(lines[max(0, i - 10):min(len(lines), i + 10)])
                # Replace non-breaking spaces with standard spaces
                concatenated = concatenated.replace('\xa0', ' ')
                match = re.search(date_pattern, concatenated, re.IGNORECASE)
                if match:
                    date_add_transaction = match.group()
                    return date_add_transaction
        return 'None'

    def find_company_name(self):
        if self.header is None:
            return None
        yes = 0
        lines = self.header.splitlines()
        for line in lines:
            if 'SUBJECT COMPANY' in line:
                yes = 1
            if 'COMPANY CONFORMED NAME' in line and yes == 1:
                company_add = line.split('\t\t\t')[-1].strip()
                return company_add

    def find_company_cik(self):
        if self.header is None:
            return None
        yes = 0
        lines = self.header.splitlines()
        for line in lines:
            if 'SUBJECT COMPANY' in line:
                yes = 1
            if 'CENTRAL INDEX KEY' in line and yes == 1:
                cik_add = line.split('\t\t\t')[-1].strip()
                return cik_add

    def find_owner_name(self):
        if self.header is None:
            return None
        yes = 0
        lines = self.header.splitlines()
        for line in lines:
            if 'FILED BY' in line:
                yes = 1
            if 'COMPANY CONFORMED NAME' in line and yes == 1:
                owner_add = line.split('\t\t\t')[-1].strip()
                return owner_add

    def find_owner_cik(self):
        if self.header is None:
            return None
        yes = 0
        lines = self.header.splitlines()
        for line in lines:
            if 'FILED BY' in line:
                yes = 1
            if 'CENTRAL INDEX KEY' in line and yes == 1:
                cik_add = line.split('\t\t\t')[-1].strip()
                return cik_add

    def find_cusip(self):
        if self.content is None:
            return 'No CUSIP'

        # Patterns with \s+ to match one or more whitespace characters
        patterns = [
            re.compile(r'\b[0-9A-Z]{1}[0-9]{3}[0-9A-Za-z]{2}[-\s]*[0-9]{0,2}[-\s]*[0-9]{0,1}\b'),
            re.compile(r'\b[0-9]{5}\s+[A-Z]\s+[0-9]{2}\s+[0-9]{1}\b'),
            re.compile(r'\b[0-9]{3}\s+[0-9]{3}\s+[0-9]{2}\s+[0-9]{1}\b'),
            re.compile(r'\b[0-9]{3}\s+[0-9]{3}\s+[0-9]{3}\b'),
            re.compile(r'\b[0-9]{9}\b'),
            re.compile(r'\b[0-9]{4}[A-Z]{1}\s+[0-9]{2}\s+[0-9]{1}\b'),
            re.compile(r'\b[0-9]{5}[A-Z][0-9]{3}\b'),
            re.compile(r'\b[0-9]{5}\s+[0-9]{2}\s+[0-9]{1}\b'),
            re.compile(r'\b[0-9A-Z]{6}\s{2}[0-9]{3}\b'),
            re.compile(r'\b\d{4} \d{5}\b')
        ]

        lines = self.content.splitlines()
        for i, line in enumerate(lines):
            if 'cusip' in line.casefold():
                for j in range(max(0, i-10), min(len(lines), i+10)):
                    for pattern in patterns:
                        match = pattern.search(lines[j])
                        if match:
                            # Remove spaces and dashes, then strip any surrounding angle brackets or other characters
                            cusip_add = re.sub(r'[\s-]+', '', match.group())
                            return cusip_add
        return 'No CUSIP'

    def find_nb_shares_agg(self):

        return find_nb_shares(self.content,
                              ['aggregate amount', 'amount beneficially owned', 'item 11'],
                              'Check if the Aggregate Amount',
                              20)

    def find_nb_shares_sole_voting(self):
        return find_nb_shares(self.content,
                              ['sole voting', 'item 7'],
                              'shared voting power',
                              20)

    def find_nb_shares_shared_voting(self):
        return find_nb_shares(self.content,
                              ['shared voting', 'item 8'],
                              'sole dispositive power',
                              20)

    def find_nb_shares_sole_dispositive(self):
        return find_nb_shares(self.content,
                              ['sole dispositive', 'sole disposition', 'item 9'],
                              'shared dispositive power',
                              20)

    def find_nb_shares_shared_dispositive(self):
        return find_nb_shares(self.content,
                              ['shared dispositive', 'shared disposition', 'item 10'],
                              'aggregate amount beneficially',
                              20)

    def find_percentage_owned(self):
        return find_percentage(self.content,
                               ['percent of class', 'class represented by', 'item 13'],
                               'type of reporting',
                               20)


if __name__ == '__main__':
    file_path = r'17_data_new/13G\2002_02\1056084_2002-02-14_000758.txt'
    file1 = FileSec(file_path)
    print(file1.find_issue_date())
    print(file1.find_transaction_date())
    print(file1.find_file_type())
    print(file1.find_company_name())
    print(file1.find_company_cik())
    print(file1.find_owner_name())
    print(file1.find_owner_cik())
    print(file1.find_cusip())
    print(file1.find_nb_shares_agg())
    print(file1.find_nb_shares_sole_voting())
    print(file1.find_nb_shares_shared_voting())
    print(file1.find_nb_shares_sole_dispositive())
    print(file1.find_nb_shares_shared_dispositive())
    print(file1.find_percentage_owned())
