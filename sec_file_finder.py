from bs4 import BeautifulSoup, NavigableString
import re

def modify_text(element):
    for content in element.contents:
        if isinstance(content, NavigableString):
            # Normalize the whitespace in the text
            new_text = ' '.join(content.strip().split())
            content.replace_with(new_text)
        elif content.contents:
            # Recursively modify text in child elements
            modify_text(content)


class TextFinder:
    def __init__(self, text=None, nb=None, html=None):
        self.text = text
        self.nb = nb
        self.html = html

    def __call__(self, file_path):

        if self.text is None and self.html is None:
            with open(file_path, 'r') as file:
                text = file.read()
            print(text)
            return

        if self.text is None and self.html is True:
            with open(file_path, 'r') as file:
                text = file.read()

            # Calculate newline to total character ratio
            newline_chars = len(re.findall(r'\n', text))
            total_chars = len(text)
            newline_ratio = newline_chars / (total_chars if total_chars > 0 else 1)

            # Parse the HTML
            soup = BeautifulSoup(text, 'lxml')
            modify_text(soup)

            # Specify block-level elements
            block_level_tags = ['p', 'div', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol', 'li', 'br', 'tr']

            # Decide whether to add newlines and spaces based on newline ratio
            print(newline_ratio)
            if newline_ratio < 0.05:
                for tag in soup.find_all(True):  # True gets all tags
                    if tag.name in block_level_tags:
                        tag.insert_after('\n')  # Add newline after block-level tags
                    else:
                        tag.insert_after(' ')  # Add space after all other tags

            # Get text with potential modifications
            modified_text = soup.get_text(separator='\n')

            # Assign this modified text to self.text or handle as needed
            print(modified_text)
            return

        if self.nb is None:
            self.nb = 10

        if self.text is not None:
            with open(file_path, 'r') as file:
                lines = file.readlines()

            found = False
            for i, line in enumerate(lines):
                if self.text in line:
                    print(f"Found text in line {i + 1}: {line.strip()}")
                    found = True
                    for j in range(1, self.nb + 1):
                        if i + j < len(lines):
                            print(lines[i + j].strip())
                    break
            if not found:
                print(f"The text '{self.text}' was not found in the file.")


def simple_finder(file_path):
    with open(file_path, 'r') as file:
        return file.read()


if __name__ == '__main__':
    find1 = TextFinder(html=True)
    # find2 = TextFinder('8-0549190')
    print(find1(r'17_data_new/13D\2016_09\1124610_2016-09-15_711017.txt'))
    # print(find2('17_data_new/13D/1994_01/51519_1994-01-06_000001'))
    # print(find2('../(17) Data_new/13D/1994_01/19411_1994-01-06_000009.txt'))
    # print(simple_finder(r'17_data_new/13D\2007_10\1166789_2007-10-02_000314.txt'))
    # python '18_shares_owned/sec_file_finder.py'
    # Renaissance technology LLC as example: 17_data_new/13G\2009_02\1037389_2009-02-12_000022.txt
