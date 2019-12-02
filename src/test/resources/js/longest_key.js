function onNode(dao) {
    var iter = dao.iteratorFromBeginning();
    var maxLength = -1;
    var record, key, keyLen;

    while (iter.hasNext()) {
        record = iter.next();
        key = record.getKey();

        keyLen = key.remaining();
        if (keyLen > maxLength) {
            maxLength = keyLen;
        }
    }

    return maxLength;
}

function onReducer(dao, results) {
    var max = -1, value;
    for (var i = 0, length = results.size(); i < length; i++) {
        value = parseInt(results.get(i));
        if (value > max) {
            max = value;
        }
    }

    return max;
}
