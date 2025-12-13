public class StreamcharService {
    Streamchar streamchar ;
    public StreamcharService(Streamchar streamchar) {
        this.streamchar = streamchar;
    }

    public String solve(String input) {
        String ans = "";
        for(int i =0;i<input.length();i++){
            char curCh = input.charAt(i);
            streamchar.add(curCh);
            ans+=streamchar.query();
        }
        return ans;
    }
}
