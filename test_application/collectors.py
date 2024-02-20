from sequential_loading import DataCollector

import httpx

#EOD Collectors

class tiingoCollector(DataCollector):

    def retrieve_data(self, domain, ticker=None):
        result = httpx.get(f"tiingo_url/{ticker}/{domain}")
        return result
    
class robinhoodCollector(DataCollector):

    def retrieve_data(self, domain, ticker=None):
        result = httpx.get(f"tiingo_url/{ticker}/{domain}")
        return result
    

# Weather Collectors
    
class weatherAPICollector(DataCollector):

    def retrieve_data(self, domain, location=None):
        result = httpx.get(f"weatherapi.com/{domain}/{location}")
        return result