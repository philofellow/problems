public class SubgraphCounting {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("USAGE: hadoop jar SubgraphCounting.jar tree.t");
            System.exit(1);
        }

        SCJobControl sc = new SCJobControl(args[0]);
        sc.Init();
        sc.SubmitJobs();
        System.exit(0);

    }
}
