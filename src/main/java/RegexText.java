import java.util.regex.Pattern;

public class RegexText {

    public static void main(String[] args) {
        String value = "'tRuMpVIruS'\t1";
//        String regex = ",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))";
//        Pattern p = Pattern.compile(regex);

        String[] data = value.split("\t");
//        String tmp1 = data[11];
//        String tmp2 = data[12];

        System.out.println(data[0]);

    }
}
