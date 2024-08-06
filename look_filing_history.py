import pandas as pd


def name_to_cik(comp_name, data):

    def name_to_cik_founder(column_name, column_cik, comp_name):
        if column_name == 'company':
            print('First looking into issuers.')
        else:
            print('No matching found. Now looking into owners.')
        mask = data[column_name].str.contains(comp_name, na=False, case=False)
        filtered_data = data.loc[mask]
        possible = list(set(zip(filtered_data[column_name], filtered_data[column_cik])))
        possible = [(index, name_cik) for index, name_cik in enumerate(possible)]
        possible_dict = dict(possible)
        if len(possible) > 1:
            comp_num = int(input(f'Which (number)? {possible}.'))
            comp_name = possible_dict[comp_num][0]
            cik = data[data[column_name] == comp_name][column_cik].values[0]
            return cik
        if len(possible) == 1:
            comp_name = possible_dict[0][0]
            cik = data[data[column_name] == comp_name][column_cik].values[0]
            return cik
        else:
            return

    type_key_pairs = [('company', 'cik'), ('owner', 'cik_owner')]
    for type_, key in type_key_pairs:
        cik = name_to_cik_founder(type_, key, comp_name)
        if cik:
            print('Found.')
            return cik

    print('No match found.')
    return None


class SharesOwned:
    def __init__(self, file_path):
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_colwidth', None)
        self.file_path = file_path
        self.data = pd.read_csv(self.file_path, dtype=str)

    def head(self):
        return self.data.head()

    def pair_story(self, subsidiary, parent):

        if not subsidiary[0].isdigit():
            subsidiary = name_to_cik(subsidiary, self.data)
        if not parent[0].isdigit():
            parent = name_to_cik(parent, self.data)

        pair_data = self.data[self.data['cik'] == subsidiary]
        pair_data = pair_data[pair_data['cik_owner'] == parent]
        if len(pair_data) == 0:
            print('No pair found.')
            return
        pair_data.date_issue = pd.to_datetime(pair_data.date_issue)
        pair_data = pair_data.sort_values(by='date_issue')

        return print(pair_data)

    def all_filings(self, comp_name, cik='cik'):

        if not comp_name[0].isdigit():
            comp_name = name_to_cik(comp_name, self.data)
        all_files = self.data[self.data[cik] == comp_name]
        if len(all_files) > 0:
            # all_files.date_issue = pd.to_datetime(all_files)
            # all_files = all_files.sort_values(by='date_issue')
            print(all_files)
            return
        else:
            print('No match found.')


if __name__ == '__main__':
    shares_owned = SharesOwned('../17_data_new/shares_owned')
    shares_owned.pair_story('Keebler', 'Flowers')

    # shares_owned.pair_story('VERITAS SOFTWARE CORP /DE/', 'SEAGATE TECHNOLOGY INC')
    # shares_owned.pair_story('HARMAN INTERNATIONAL INDUSTRIES INC /DE/', 'WADDELL & REED FINANCIAL INC')
    # shares_owned.pair_story('Inergy Midstream', 'Inergy')
    # python '18_shares_owned/look_filing_history.py'
    # shares_owned.pair_story('', '')

    # Found:

    # From Anderson and Jansen (2022)
    # shares_owned.pair_story('uBID', 'Creative Computers')
    # shares_owned.pair_story('XM Satellite', 'American Mobile')
    # shares_owned.pair_story('XLConnect', 'Intelligent Electronics')
    # shares_owned.pair_story('United Pan', 'United International')
    # shares_owned.pair_story('Trex Medical', 'ThermoTrex')
    # shares_owned.pair_story('TerraForm', 'SunEdison')
    # shares_owned.pair_story('Symons', 'Goran Capita')
    # shares_owned.pair_story('SunEdison Semiconductor', 'SUNEDISON, INC.') -> Filing issued on
    # February 17, 2015. SunEdison Semiconductor stopped trading on December 2016. Started in 2014.
    # shares_owned.pair_story('Ruthigen', 'Oculus Innovative')-> there is switch form 13G to 13D.
    # shares_owned.pair_story('Republic Service', 'Republic Industries') -> All in 13D
    # shares_owned.pair_story('Reliant Resource', 'Reliant Energy') -> Reliant Resource has two cik. For both
    # cik I have the same filing.
    # shares_owned.pair_story('Omega Protein', 'Zapata')
    # shares_owned.pair_story('Midway Games', 'WMS Industries')
    # shares_owned.pair_story('Metris', 'Fingerhut')
    # shares_owned.pair_story('Mego Mortgage', 'Mego Financial') -> On September 2, 1997,
    # the Company distributed all of its 10 million shares of common stock of its former subsidiary,
    # Mego Mortgage Corporation (MMC) to the Company's shareholders in a tax-free Spin-off.There is
    # no amendment to inform investors that Mego Financial doesn't hold the shares anymore. Maybe because
    # it is only in 1998 that the SEC told that amendments of 13G when owning more than 20% have to be
    # done within 10 days.
    # shares_owned.pair_story('Logility', 'American Software')
    # shares_owned.pair_story('Instinet Group', 'Reuters Group') # -> some issues, shares_agg (solved)
    # shares_owned.pair_story('Inergy Midstream', 'Inergy')
    # shares_owned.pair_story('Independence Rea', 'RAIT Financial')
    # shares_owned.pair_story('Document Science', 'Xerox Corp')
    # shares_owned.pair_story('Coach Inc', 'Sara Lee Corp')
    # shares_owned.pair_story('CBS Corp', 'CBS Corp') -> Yes, but parent and sub have the same cik.
    # shares_owned.pair_story('Alon USA Partner', 'Alon USA Energy')
    # shares_owned.pair_story('American Capital Agency', 'American Capital Strategies') # (most likely)
    # shares_owned.pair_story('BANCO SANTANDER CHILE', 'BANCO SANTANDER CENTRAL HISPANO SA')
    # shares_owned.pair_story('Box Ships', 'Paragon Shipping Inc.')
    # shares_owned.pair_story('Brookdale Senior', 'Fortress Investment')

    # From Cornell and Liu (2001)
    # shares_owned.pair_story('VERITAS SOFTWARE CORP /DE/', 'SEAGATE TECHNOLOGY INC')
    # shares_owned.pair_story('Careinsite', 'Medical Manager')
    # shares_owned.pair_story('IXnet', 'IPC communications')
    # shares_owned.pair_story('intimate', 'limited inc')
    # shares_owned.pair_story('Keebler', 'Flowers')
    # shares_owned.pair_story('HOWMET', 'CORDANT')

    # Not found:

    # From Anderson and Jansen (2022)
    # shares_owned.pair_story('Xpedior', 'Metamor') -> didn' search
    # shares_owned.pair_story('US Search', 'Kushner–Locke') -> issues with names.
    # shares_owned.pair_story('TransAct Technol', 'Tridex') -> TransAct was incorporated in June 1996 and began
    # operating as a stand-alone business in August 1996 as a spin-off of the printer business that was formerly
    # conducted by certain subsidiaries of Tridex Corporation. We completed an initial public offering on
    # August 22, 1996. As of the date of this report, Tridex owns 5,400,000 shares, or approximately 80.3%, of
    # the outstanding common stock of TransAct Technologies Incorporated ("TransAct"). Tridex has announced that
    # on March 31, 1997 it intends to distribute those shares pro rata to persons who were Tridex stockholders of
    # record on March 14, 1997. On March 31, 1997 the Company effected the previously announced distribution of
    # 5,400,000 shares of common stock of its former subsidiary, TransAct Technologies Incorporated ("TransAct")
    # to Tridex stockholders on the basis of 1.005 shares of TransAct common stock for each share of Tridex common
    # stock owned.
    # shares_owned.pair_story('Tim Hortons', 'Wendy') -> No research
    # shares_owned.pair_story('Shochet', 'Research Partners') -> Naming issue. There is a big holder of
    # Shochet but he is named Fireband Financial Group Inc. So maybe I have it.
    # shares_owned.pair_story('Riverstone', 'Cabletron') -> On February 16, 2001, Riverstone completed the
    # initial public offering of 10.0 million shares of common stock of Riverstone at $12 per share. Prior
    # to the offering, Riverstone was a wholly owned subsidiary of Cabletron. Cabletron continued to own
    # approximately 85 percent of Riverstone subsequent to the offering and exercise by the strategic investors
    # of the stock purchase. On March 28, 2001, Cabletron announced that it had received a private letter ruling
    # from the Internal Revenue Service that the distribution of Cabletron's shares of Riverstone would be
    # tax-free to to Cabletron's stockholders. On July 17, 2001, the Company’s Board of Directors declared a
    # special dividend of the Company’s shares of Riverstone common stock to the Company’s shareholders of
    # record on July 27, 2001, payable on August 6, 2001. On August 6, 2001, the Company distributed its
    # shares of Riverstone common stock to the Company’s shareholders. The distribution ratio was 0.5131 shares
    # of Riverstone common stock for each outstanding share of the Company’s common stock. No gain or loss was
    # recorded as a result of these transactions. As a result of the distribution of the Company’s Riverstone
    # shares, the Company recorded a non-cash charge to retained earnings of $329.6 million which reflected the
    # net assets and liabilities of Riverstone.
    # shares_owned.pair_story('Novacare employe', 'NovaCare') -> Could find NovaCare but no NovaCare Employee
    # even on Edgard.
    # shares_owned.pair_story('MIPS', 'Silicon Graphics') -> On July 6, 1998 the company (Silicon Graphics)
    # closed and initial public offering of the common stock to its subsidiary, MIPS Technologies, Inc.
    # And at June 30, 1999, Silicon Graphics still owns 67% of outstanding shares. I have no good explanation
    # other than a miss-understanding of the rules.
    # shares_owned.pair_story('Ferrari', 'Fiat Chrysler') -> Similarly to 3COM/Palm, however I do not
    # know yet if the shares were distributed within the year or if the absence of filing is due to the
    # fact that both companies do not have the headquarters in the US. This is unlikely !
    # The IPO occurred on October 21st 2015 at USD 52 per common shares. And the spin-off was completed
    # on January 3, 2016. It is likely that if the parent intends to spin-off directly the shares of the
    # subsidiary, it doesn't need to file a 13G filing at the end of the year.
    # See: https://www.stellantis.com/content/dam/stellantis-corporate/archives/fca/past-corporate-transactions/ferrari-separation/Ferrari_Separation_QA.pdf
    # shares_owned.pair_story('FMC Technologies', 'FMC Corp') -> Similarly to 3COM/Palm, the shares
    # were distributed before the end of the year. Therefore, no 13G was required after the spin-off.
    # Everything is explained here: https://www.sec.gov/Archives/edgar/data/37785/000095013101503999/d10q.txt
    # shares_owned.pair_story('Cognizant Tech S', 'Cognizant Corp') -> there might be a name change
    # it's IMS HEALTH INC that owns 61.7% of Cognizant Tech S at the IPO.
    # shares_owned.pair_story('American Nationa', 'Pechiney SA') -> Pechiney is French and
    # American Nationa isn't really clear.
    # shares_owned.pair_story('ATL', 'Odetics') -> ATL Products is in the NL.
    # shares_owned.pair_story('Palm', '3 Com') -> see comment below for the Cornell and Liu (2001) paper.

    # From Cornell and Liu (2001)
    # shares_owned.pair_story('Palm', '3 Com') -> it seems like parent didn't neet to file a 13D
    # filing when there is an equity carve out. When shares are already held when the issuer is
    # going public, filing the 13D filing isn't necessary. You only have to file the 13G filing
    # at the end of the year in which the issuer went public. (Maybe the rule did evolve?) But
    # the Palm vs 3 COM lasted only 5 months (from the 2 Mars, 2000 to July 27, 2000). This may
    # explain why no corresponding filing have been found.
    # In Anderson and Jansen (2022), they say that the length of the pair was 2543 days with 647
    # days with a negative stub value. Lamont and Thaler (2003) give the same period of 5 months.
    # So there seems to be some big approximation in Anderson and Jansen (2003).
