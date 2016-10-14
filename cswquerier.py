from owslib.csw import CatalogueServiceWeb
from owslib.fes import PropertyIsEqualTo, Not, Or, And


class CSWQuerier:

    max_records = 100
    is_dataset = PropertyIsEqualTo("Type", "dataset")
    non_havested = PropertyIsEqualTo("_isHarvested", "n")

    def __init__(self, url, username=None, password=None):
        self.csw = CatalogueServiceWeb(url, username=username, password=password)
        self.mds_not_parsable = []
        self.reset()

    def reset(self):
        self.start = 0
        self.md_count = -1

    def get_records(self):
        try:
            self.csw.getrecords2(constraints=self.generate_filter(),
                                 esn='full',
                                 startposition=self.start,
                                 maxrecords=self.max_records)
            print("CSWQuerier.get_records() results : %s (start=%s, max=%s)" % (self.csw.results, self.start, self.max_records))
            self.start += self.csw.results['returned']
        except ValueError:
            self.search_for_error()
            return self.get_records()

        return self.csw.records

    def get_md(self, uuid):
        return self.csw.records[uuid]

    def search_for_error(self):
        index = self.start
        while index < self.start + self.max_records:
            try:
                self.csw.getrecords2(constraints=self.generate_filter(), esn='full', startposition=index, maxrecords=1)
                print("Index : %s" % index)
                index += 1
            except ValueError:
                self.csw.getrecords2(constraints=self.generate_filter(), startposition=index, maxrecords=1)
                for uuid in self.csw.records:
                    self.mds_not_parsable.append(uuid)
                    print("-----------------------------------------------------------------------------------------------------------------------------------> Error on %s at %s" % (uuid, index))
                    return

                return ValueError("Unable to find bogus MD")

    def generate_filter(self):
        if len(self.mds_not_parsable) == 0:
            filters = [self.is_dataset, self.non_havested]
            return [self.is_dataset]
        elif len(self.mds_not_parsable) == 1:
            filters = [self.is_dataset,
                       # self.non_havested,
                       Not([PropertyIsEqualTo("truite", self.mds_not_parsable[0])])]
        else:
            filters = [self.is_dataset,
                       # self.non_havested,
                       Not(Or([PropertyIsEqualTo("fileIdentifier", uuid) for uuid in self.mds_not_parsable]))]
        return [And(filters)]



