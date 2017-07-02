package com.semantix;

/**
 * Created by semantix on 01/07/17.
 */
public class Translate {
    public static int[] getData(String data) {
        int[] value = null;
        switch(data) {
            case "station":
                value = new int[]{0, 6};
                break;
            case "year":
                value = new int[]{14, 18};
                break;
            case "month":
                value = new int[]{18, 20};
                break;
            case "day":
                value = new int[]{20, 22};
                break;
            case "yearMonth":
                value = new int[]{14, 20};
                break;
            case "monthDay":
                value = new int[]{18, 22};
                break;
            case "temp":
                value = new int[]{24, 30};
                break;
            case "dewPoint":
                value = new int[]{35, 41};
                break;
            case "seaLevelPressure":
                value = new int[]{46, 52};
                break;
            case "stationPressure":
                value = new int[]{57, 63};
                break;
            case "visibility":
                value = new int[]{68, 73};
                break;
            case "windSpeed":
                value = new int[]{78, 83};
                break;
            case "maxWindSpeed":
                value = new int[]{88, 93};
                break;
            case "maxWindGust":
                value = new int[]{95, 100};
                break;
            case "maxTemp":
                value = new int[]{102, 108};
                break;
            case "minTemp":
                value = new int[]{110, 116};
                break;
            case "precipitation":
                value = new int[]{118, 123};
                break;
            case "snowDepth":
                value = new int[]{125, 130};
                break;
            case "fog":
                value = new int[]{132, 133};
                break;
            case "rain":
                value = new int[]{133, 134};
                break;
            case "snow":
                value = new int[]{134, 135};
                break;
            case "hail":
                value = new int[]{135, 136};
                break;
            case "thunder":
                value = new int[]{136, 137};
                break;
            case "tornado":
                value = new int[]{138, 139};
                break;
        }
        return value;
    }
}