import java.util.regex.Pattern;

public class RegexText {

    public static void main(String[] args) {
        String value = "2020-10-15 00:00:01,1.316529221557252e+18,0.0,0.0,TweetDeck,360666534.0,El Sol Latino News,elsollatinonews,2011-08-23 15:33:45,1860.0,25.77427,-80.19366,,United States of America,North America,Florida,\"['Elecciones2020', 'En', 'Florida', 'JoeBiden', 'dice', 'que', 'DonaldTrump', 'solo', 'se', 'preocupa', 'por', 'él', 'mismo', 'El', 'demócrata', 'fue', 'anfitrión', 'de', 'encuentros', 'de', 'electores', 'en', 'PembrokePines', 'Miramar', 'Clic', 'AQUÍ', '⬇️⬇️⬇️', '⠀', '\uD83C\uDF10https', 'tcoqhIWpIUXsT', 'ElSolLatino', 'yobrilloconelsol', 'https', 'tco6FlCBWf1Mi']\",\"['\uD83C\uDF10', 'Noticias', 'de', 'interés', 'para', 'latinos', 'de', 'la', 'costa', 'este', 'de', 'EEUU', '⠀\u23F9️', 'Facebook', 'e', 'Instagram', '⠀\uD83C\uDFD9️', 'Philadelphia', 'elsollatinonewspaper', '⠀\uD83C\uDF05', 'Miami', 'elsollatinonewsmiami']\"";
        String regex = ",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))";
        Pattern p = Pattern.compile(regex);

        String[] data = p.split(value);
//        String[] data = value.split("\t");
//        String tmp1 = data[11];
//        String tmp2 = data[12];

        System.out.println(data[17].substring(2, data[17].length() - 2));

    }
}
