import re
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize


# nltk.download('stopwords')
# nltk.download('punkt')
class Preprocessor:
    def __init__(self, stop_words=True, stemming=True, lowercasing=True, special_chars_removal=True,
                 numbers_removal=True, spell_check=False):
        self.stop_words = stop_words
        self.stemming = stemming
        self.lowercasing = lowercasing
        self.special_chars_removal = special_chars_removal
        self.numbers_removal = numbers_removal
        self.spell_check = spell_check

        if self.stop_words:
            self.stopwords = set(stopwords.words('english'))

        if self.stemming:
            self.stemmer = PorterStemmer()

    def preprocess(self, text):
        if self.lowercasing:
            text = text.lower()

        words = word_tokenize(text)

        if self.stop_words:
            words = [word for word in words if word not in self.stopwords]

        if self.stemming:
            words = [self.stemmer.stem(word) for word in words]

        if self.special_chars_removal:
            words = [re.sub('[^A-Za-z]+', '', word) for word in words]

        if self.numbers_removal:
            words = [re.sub(r'\d+', '', word) for word in words]

        if self.spell_check:
            # Add spell checking and correction code here
            pass

        return ' '.join(words)
