#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <time.h>
#include <ctype.h>
#include <mysql.h>

#ifndef my_bool
typedef char my_bool;
#endif

static void trim(char *s) {
    char *p = s;
    while (isspace((unsigned char)*p))
        p++;
    if (p != s)
        memmove(s, p, strlen(p) + 1);
    char *end = s + strlen(s) - 1;
    while (end >= s && isspace((unsigned char)*end)) {
        *end = '\0';
        end--;
    }
}

/*
 * parse_field() zerlegt ein einzelnes Cron-Feld in eine Bitmaske der erlaubten Werte.
 * Falls das Feld "*" ist, wird is_all gesetzt.
 * Bei Feldteilen mit "/" und ggf. "-" wird der Bereich und Schritt ausgelesen.
 * Für alle Felder (auch dow) wird keine zusätzliche Umrechnung vorgenommen.
 */
static void parse_field(const char *field, int min, int max, int valid[], int *is_all, int is_dow) {
    int i;
    *is_all = 0;
    for (i = min; i <= max; i++)
        valid[i] = 0;
    if (strcmp(field, "*") == 0) {
        *is_all = 1;
        for (i = min; i <= max; i++)
            valid[i] = 1;
        return;
    }
    char buf[256];
    strncpy(buf, field, sizeof(buf)-1);
    buf[sizeof(buf)-1] = '\0';
    char *token = strtok(buf, ",");
    while (token != NULL) {
        char tok[64];
        strncpy(tok, token, sizeof(tok)-1);
        tok[sizeof(tok)-1] = '\0';
        trim(tok);
        int step = 1, start_val, end_val;
        char *slash = strchr(tok, '/');
        if (slash) {
            *slash = '\0';
            step = atoi(slash+1);
            if (step <= 0)
                step = 1;
            char *dash = strchr(tok, '-');
            if (dash) {
                *dash = '\0';
                start_val = atoi(tok);
                end_val = atoi(dash+1);
            } else {
                start_val = atoi(tok);
                end_val = max;
            }
        } else {
            char *dash = strchr(tok, '-');
            if (dash) {
                *dash = '\0';
                start_val = atoi(tok);
                end_val = atoi(dash+1);
            } else {
                start_val = atoi(tok);
                end_val = start_val;
            }
        }
        if (start_val < min)
            start_val = min;
        if (end_val > max)
            end_val = max;
        for (i = start_val; i <= end_val; i += step)
            valid[i] = 1;
        token = strtok(NULL, ",");
    }
}

typedef struct {
    int sec[60];
    int min[60];
    int hour[24];
    int dom[32];    /* 1..31 */
    int month[13];  /* 1..12 */
    int dow[7];     /* 0..6, hier: Sonntag=0, Montag=1, … */
    int dom_all;
    int month_all;
    int dow_all;
} CronSchedule;

static int parse_cron(const char *expr, CronSchedule *schedule) {
    char expr_copy[256];
    strncpy(expr_copy, expr, sizeof(expr_copy)-1);
    expr_copy[sizeof(expr_copy)-1] = '\0';
    char *fields[6];
    int i = 0;
    char *token = strtok(expr_copy, " ");
    while (token && i < 6) {
        fields[i++] = token;
        token = strtok(NULL, " ");
    }
    if (i != 6)
        return -1;  /* Exakt 6 Felder erforderlich */
    parse_field(fields[0], 0, 59, schedule->sec, &(int){0}, 0);
    parse_field(fields[1], 0, 59, schedule->min, &(int){0}, 0);
    parse_field(fields[2], 0, 23, schedule->hour, &(int){0}, 0);
    parse_field(fields[3], 1, 31, schedule->dom, &(schedule->dom_all), 0);
    parse_field(fields[4], 1, 12, schedule->month, &(schedule->month_all), 0);
    parse_field(fields[5], 0, 6, schedule->dow, &(schedule->dow_all), 1);
    return 0;
}

static time_t next_execution(const CronSchedule *sch, time_t now) {
    struct tm today_tm;
    localtime_r(&now, &today_tm);
    today_tm.tm_hour = 0;
    today_tm.tm_min = 0;
    today_tm.tm_sec = 0;
    time_t today = mktime(&today_tm);
    int days;
    for (days = 0; days < 366; days++) {
        time_t candidate_day = today + days * 86400;
        struct tm cand_tm;
        localtime_r(&candidate_day, &cand_tm);
        int cand_month = cand_tm.tm_mon + 1;
        int cand_dom = cand_tm.tm_mday;
        /* Verwende tm_wday direkt: Sonntag=0, Montag=1, … */
        int cand_dow = cand_tm.tm_wday;
        if (!sch->month[cand_month])
            continue;
        int valid_dom = sch->dom_all ? 0 : sch->dom[cand_dom];
        int valid_dow = sch->dow_all ? 0 : sch->dow[cand_dow];
        if (!sch->dom_all && !sch->dow_all) {
            if (!(valid_dom || valid_dow))
                continue;
        } else if (!sch->dom_all && !valid_dom) {
            continue;
        } else if (!sch->dow_all && !valid_dow) {
            continue;
        }
        int start_hour, start_min, start_sec;
        if (candidate_day == today) {
            struct tm now_tm;
            localtime_r(&now, &now_tm);
            start_hour = now_tm.tm_hour;
            start_min  = now_tm.tm_min;
            start_sec  = now_tm.tm_sec;
        } else {
            start_hour = 0; start_min = 0; start_sec = 0;
        }
        int h, m, s;
        for (h = start_hour; h < 24; h++) {
            if (!sch->hour[h])
                continue;
            for (m = (h == start_hour ? start_min : 0); m < 60; m++) {
                if (!sch->min[m])
                    continue;
                for (s = (h == start_hour && m == start_min ? start_sec : 0); s < 60; s++) {
                    if (sch->sec[s]) {
                        struct tm res_tm = cand_tm;
                        res_tm.tm_hour = h;
                        res_tm.tm_min  = m;
                        res_tm.tm_sec  = s;
                        time_t res_time = mktime(&res_tm);
                        if (res_time >= now)
                            return res_time;
                    }
                }
            }
        }
    }
    return -1;
}

my_bool cron_next_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    if (args->arg_count != 1 && args->arg_count != 2) {
        strcpy(message, "1 oder 2 Argumente erwartet");
        return 1;
    }
    if (args->arg_type[0] != STRING_RESULT) {
        strcpy(message, "1. Argument muss ein String (Cron-Ausdruck) sein");
        return 1;
    }
    if (args->arg_count == 2 && args->arg_type[1] != STRING_RESULT) {
        strcpy(message, "2. Argument (Startzeit) muss ein String sein");
        return 1;
    }
    initid->maybe_null = 1;
    initid->max_length = 64;
    return 0;
}

char *cron_next(UDF_INIT *initid, UDF_ARGS *args,
                char *result, unsigned long *length,
                char *is_null, char *error) {
    const char *cron_expr = args->args[0];
    if (cron_expr == NULL) {
        *is_null = 1;
        return NULL;
    }

    time_t start_time;
    if (args->arg_count == 2 && args->args[1] != NULL) {
        struct tm start_tm;
        memset(&start_tm, 0, sizeof(start_tm));
        if (strptime(args->args[1], "%Y-%m-%d %H:%M:%S", &start_tm) != NULL) {
            start_tm.tm_isdst = -1;
            start_time = mktime(&start_tm);
        } else {
            *is_null = 1;
            return NULL;
        }
    } else {
        start_time = time(NULL);
    }

    CronSchedule schedule;
    if (parse_cron(cron_expr, &schedule) != 0) {
        *is_null = 1;
        return NULL;
    }

    time_t next = next_execution(&schedule, start_time);
    if (next == -1) {
        *is_null = 1;
        return NULL;
    }
    struct tm res_tm;
    localtime_r(&next, &res_tm);
    snprintf(result, 64, "%04d-%02d-%02d %02d:%02d:%02d",
             res_tm.tm_year + 1900, res_tm.tm_mon + 1, res_tm.tm_mday,
             res_tm.tm_hour, res_tm.tm_min, res_tm.tm_sec);
    *length = strlen(result);
    return result;
}

void cron_next_deinit(UDF_INIT *initid) {
}
