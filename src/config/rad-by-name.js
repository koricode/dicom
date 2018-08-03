var map = function (doc) {
    if (("0008002A" in doc || ("00080022" in doc && "00080032" in doc)) && "00080060" in doc) {
        var value = {}, isEmit = false;

        ["00180060", "00181150", "00181151", "00189321", "00189345", "00181405"].forEach(function (tag) {
            if (tag in doc) {
                value[tag] = doc[tag];
                isEmit = true;
            }
        });

        if (isEmit) {
            var modality = doc["00080060"],
                datetime = "0008002A" in doc ? doc["0008002A"] : doc["00080022"] + doc["00080032"];

            var yr = Number(datetime.substr(0, 4)),
                mo = Number(datetime.substr(4, 2)),
                da = Number(datetime.substr(6, 2)),
                hr = Number(datetime.substr(8, 2)),
                mn = Number(datetime.substr(10, 2)),
                sc = Number(datetime.substr(12, 2));

            emit([modality, yr, mo, da, hr, mn, sc], value);
        }
    }
};

var map = function (doc) {
    if (("0008002A" in doc || ("00080022" in doc && "00080032" in doc)) && "00080060" in doc && "00100020" in doc) {
        var value = {}, isEmit = false;

        ["00180060", "00181150", "00181151", "00189321", "00189345", "00181405"].forEach(function (tag) {
            if (tag in doc) {
                value[tag] = doc[tag];
                isEmit = true;
            }
        });

        if (isEmit) {
            var patient = doc["00100020"],
                modality = doc["00080060"],
                datetime = "0008002A" in doc ? doc["0008002A"] : doc["00080022"] + doc["00080032"];

            var yr = Number(datetime.substr(0, 4)),
                mo = Number(datetime.substr(4, 2)),
                da = Number(datetime.substr(6, 2)),
                hr = Number(datetime.substr(8, 2)),
                mn = Number(datetime.substr(10, 2)),
                sc = Number(datetime.substr(12, 2));

            emit([patient, modality, yr, mo, da, hr, mn, sc], value);
        }
    }
};