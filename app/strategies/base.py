from abc import ABC, abstractmethod


class BaseSearchStrategy(ABC):

    def __init__(self, page, base_url):
        self.page = page
        self.base_url = base_url

    @abstractmethod
    async def execute(self):
        pass