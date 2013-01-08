from scrapy.contracts import Contract
from scrapy.exceptions import ContractFail

class FeedHasBeenUpdateWithinSixMonthsContract(Contract):
    name="feed_has_been_updated_within_six_months"