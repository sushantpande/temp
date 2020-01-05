from datetime import datetime
import calendar

def reduce_day_per_char(sd, ed):
    ret = list()
    if ed <=9:
        if ed == 1:
            ret.append('%d' % ed)
        else:
            ret.append('[%d-%d]' % (sd, ed))
    elif ed <=19:
        if sd <= 9:
            if sd == 9:
                ret.append('%d' % sd)
            else:
                ret.append('[%d-%d]' % (sd, 9))
            if ed == 10:
                ret.append('%d' % ed)
            else:
                ret.append('1[%d-%d]' % (0, ed-10))
        else:
            ret.append('1[%d-%d]' % (sd-10, ed-10))
    elif ed <=29:
        if sd <= 9:
            if sd == 9:
                ret.append('%d' % sd)
            else:
                ret.append('[%d-%d]' % (sd, 9))
            ret.append('1*')
            if ed == 20:
                ret.append('%d' % ed)
            else:
                ret.append('2[%d-%d]' % (0, ed-20))
        elif sd <=19:
            if sd == 19:
                ret.append('%d' % sd)
            else:
                ret.append('1[%d-%d]' % (sd-10, 9))
            if ed == 20:
                ret.append('%d' % ed)
            else:
                ret.append('2[%d-%d]' % (0, ed-20))
        else:
            ret.append('2[%d-%d]' % (sd-20, ed-20))
    else:
        if sd <= 9:
            if sd == 9:
                ret.append('%d' % sd)
            else:
                ret.append('[%d-%d]' % (sd, 9))
            ret.append('1*')
            ret.append('2*')
            if ed == 30:
                ret.append('%d' % ed)
            else:
                ret.append('3[%d-%d]' % (0, ed-30))
        elif sd <=19:
            if sd == 19:
                ret.append('%d' % sd)
            else:
                ret.append('1[%d-%d]' % (sd-10, 9))
            ret.append('2*')
            if ed == 30:
                ret.append('%d' % ed)
            else:
                ret.append('3[%d-%d]' % (0, ed-30))
        elif sd <= 29:
            if sd == 29:
                ret.append('%d' % sd)
            else:
                ret.append('2[%d-%d]' % (sd-20, 9))
            if ed == 30:
                ret.append('%d' % ed)
            else:
                ret.append('3[%d-%d]' % (0, ed-30))
        else:
            ret.append('3[%d-%d]' % (sd-30, ed-30))
    return ret


def reduce_month_per_char(sm, em):
    ret = list()
    if em <=9:
        if em == 1:
            ret.append('%d' % em)
        else:
            ret.append('[%d-%d]' % (sm, em))
    else:
        if sm <= 9:
            if sm == 9:
                ret.append('%d' % sm)
            else:
                ret.append('[%d-%d]' % (sm, 9))
            if em == 10:
                ret.append('%d' % em)
            else:
                ret.append('1[%d-%d]' % (0, em-10))
        else:
            ret.append('1[%d-%d]' % (sm-10, em-10))

    return ret

def generate_for_a_day(year, month, day):
    return '%d/%d/%d' % (year, month, day)

def generate_for_a_month(year, month, sd_day, ed_day):
    ret_list = list()
    ym = '%d/%d' % (year, month)
    day_list = reduce_day_per_char(sd_day, ed_day)
    for day in day_list:
        ret_list.append('%s/%s' % (ym, day))

    return ','.join(ret_list)

def generate_for_months(year, sd_month, sd_day, ed_month, ed_day):
    (s_month, it_months, e_month, sm, em) = ('', '', '', sd_month+1, ed_month-1)
    if sd_day == 1:
        sm -= 1
    else:
        if sd_day == calendar.monthrange(year, sd_month)[1]:
            s_month = generate_for_a_day(year, sd_month, sd_day)
        else:
            s_month = generate_for_a_month(year, sd_month, sd_day, calendar.monthrange(year, sd_month)[1])

    if ed_day == calendar.monthrange(year, ed_month)[1]:
        em += 1
    else:
        if ed_day == 1:
            e_month = generate_for_a_day(year, ed_month, ed_day)
        else:
            e_month = generate_for_a_month(year, ed_month, 1, ed_day)

    if sm == em:
        it_months = '%d/%d/*' % (year, sm)
    elif em - sm >= 1:
        tmp = list()
        for mon in reduce_month_per_char(sm, em):
            tmp.append('%d/%s/*' % (year, mon))
        it_months = ','.join(tmp)

    ret = ''
    for s in [s_month, it_months, e_month]:
        if s:
            ret += '%s,' % s
    ret = ret.strip(',')
    return ret
        
def generate(st, et):
    if st > et:
        raise ValueError("End timestamp must be greater than the start timestamp")
    
    ret = str()
    
    sd = datetime.utcfromtimestamp(st)
    ed = datetime.utcfromtimestamp(et)
    (sd_year, sd_month, sd_day) = (sd.year, sd.month, sd.day)
    (ed_year, ed_month, ed_day) = (ed.year, ed.month, ed.day)
    
    if sd_year == ed_year and sd_month == ed_month and sd_day == ed_day:
        ret = '{%s}' % generate_for_a_day(sd_year, sd_month, sd_day)
    else:
        if sd_year == ed_year:
            if sd_month == ed_month:
                if sd_day == 1 and ed_day == calendar.monthrange(sd_year, ed_month)[1]:
                    ret = '{%d/%d/*}' % (sd_year, sd_month)
                else:
                    ret = '{%s}' % generate_for_a_month(sd_year, sd_month, sd_day, ed_day)
            else:
                if sd_month == 1 and sd_day == 1 and ed_month == 12 and ed_day == calendar.monthrange(sd_year, ed_month)[1]:
                    ret = '{%d/*/*}' % (sd_year)
                else:
                    ret = '{%s}' % generate_for_months(sd_year, sd_month, sd_day, ed_month, ed_day)
        else:
            (s_year, it_years, e_year, sy, ey) = ('', '', '', sd_year+1, ed_year-1)
            if sd_month == 1 and sd_day == 1:
                sy -= 1
            else:
                if sd_month == 12:
                    if sd_day == calendar.monthrange(sd_year, 12)[1]:
                        s_year = generate_for_a_day(sd_year, sd_month, sd_day)
                    else:
                        s_year = generate_for_a_month(sd_year, sd_month, sd_day, calendar.monthrange(sd_year, 12)[1])
                else:
                    s_year = generate_for_months(sd_year, sd_month, sd_day, 12, calendar.monthrange(sd_year, 12)[1])
            
            if ed_month == 12 and ed_day == calendar.monthrange(ed_year, 12)[1]:
                ey += 1
            else:
                if ed_month == 1:
                    if ed_day == 1:
                        e_year = generate_for_a_day(ed_year, ed_month, ed_day)
                    else:
                        e_year = generate_for_a_month(ed_year, 1, 1, ed_day)
                else:
                    e_year = generate_for_months(ed_year, 1, 1, ed_month, ed_day)
                
            if sy == ey:
                it_years = '%d/*/*' % sy
            elif ey - sy >= 1:
                tmp = list()
                for yr in range(sy, ey+1):
                    tmp.append('%d/*/*' % yr)
                it_years = ','.join(tmp)

            ret = '{'
            for s in [s_year, it_years, e_year]:
                if s:
                    ret += '%s,' % s
            ret = ret.strip(',')
            ret += '}'

    return ret
