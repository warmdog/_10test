import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main11 {
    public static void main(String[] args){
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        int ans = 0, x;
        for(int i = 0; i < n; i++){
            int length =  sc.nextInt();
            for (int j = 0; j < length; j++) {
                List<Integer> part = new ArrayList<>();
                part.add(sc.nextInt());
            }
        }
    }
    public static Integer getMin(ArrayList<Integer> part ){
        int res =0;
        int max = part.get(part.size()-1);
        int second_max = 0;
        int min = part.get(0);
        int second_min = 0;
        if(part.size()>1){
            second_max = part.get(part.size()-2);
            second_min = part.get(2);
        }
        int min_diff = Math.min((max-second_max),(second_min-min));



        return res;
    }
}
