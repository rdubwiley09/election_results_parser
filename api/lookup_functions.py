def lookup_office_id(officeType, district):
    try:
        district = int(district)
    except Exception:
        return 1,0
    if officeType == "president":
        return 1,0
    if officeType == 'governor':
        return 2,0
    if officeType == 'sos':
        return 3,0
    if officeType == 'ag':
        return 4,0
    if officeType == "senate":
        return 5,0
    if officeType == "congress":
        if district<15 and district>0:
            return 6,100*district
        else:
            return 6,100
    if officeType == "misenate":
        if district<39 and district>0:
            return 7,100*district
        else:
            return 7,200
    if officeType == "mihouse":
        if district<111 and district>0:
            return 8,100*district
        else:
            return 8, 100
    if officeType == "miboe":
        return 9, 0
    if officeType == "regentum":
        return 10, 0
    if officeType == "regentmsu":
        return 11,0
    if officeType == "wsugovernor":
        return 12,0
    if officeType == "miscotus":
        return 13,0
    return 1,0
