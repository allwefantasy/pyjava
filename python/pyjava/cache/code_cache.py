class CodeCache(object):
    cache = {}
    cache_max_size = 10 * 10000

    @staticmethod
    def get(code):
        if code in CodeCache.cache:
            return CodeCache.cache[code]
        else:
            return CodeCache.gen_cache(code)

    @staticmethod
    def gen_cache(code):
        if len(CodeCache.cache) > CodeCache.cache_max_size:
            CodeCache.cache.clear()
        CodeCache.cache[code] = compile(code, '<string>', 'exec')
        return CodeCache.cache[code]
