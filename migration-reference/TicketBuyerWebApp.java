import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.Headers;
import java.time.LocalDate;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.text.NumberFormat;
import java.util.Locale;

/**
 * The One Room   Database Manager
 *
 * Algorithm inventory (all run inside /dataInsights):
 *
 *  GRAPH THEORY
 *    - Bipartite contact-show graph construction
 *    - Jaccard similarity  J(A,B) = |A∩B| / |A∪B|  between every show pair
 *    - BFS connected-components on the contact projection
 *    - Degree centrality (unique-attendee count per show)
 *    - Bridgeness proxy (shared-contact pairs bridged per show)
 *    - Cross-genre fan detection (contacts spanning ≥2 categories)
 *
 *  STATISTICS
 *    - Pearson correlation r between ticket volume and unique-contact count
 *    - Chi-squared goodness-of-fit on booking time-of-day distribution (df=3)
 *    - p-value approximation via chi-squared CDF table
 *    - 3-period revenue moving average
 *    - Z-score outlier detection on per-show ticket volumes
 *    - Cohort retention rate
 *
 *  GREEDY
 *    - Set-cover approximation: minimum shows to reach TARGET_COVERAGE of audience
 *      (greedy max-marginal; achieves ln(n)+1 approximation guarantee)
 *    - Greedy category reach-order optimisation
 *
 *  DYNAMIC PROGRAMMING
 *    - 0/1 Knapsack: maximise unique audience within a half-season slot budget  O(n·W)
 *    - LCS-based audience-similarity ranking between show pairs  O(m·n) per pair
 */
public class TicketBuyerWebApp {

    private static final File FRONTEND_DIST = new File("frontend", "dist");

    private static final String[] CATEGORIES = {
            "Afrosoul", "Jazz", "Comedy", "Folk", "HipHop", "Reggae"
    };

    private static final Object DATA_CACHE_LOCK = new Object();
    private static volatile String cachedDataRevision;
    private static volatile ProcessedData cachedProcessedData;

    // ================================================================
    //  DATA STRUCTURES
    // ================================================================

    private static class ProcessedData {
        List<String>       showNames;
        List<List<String>> showEmailsLists;
        List<List<String>> showPhoneLists;

        List<String> catAfroEmails, catJazzEmails, catComedyEmails;
        List<String> catPoetryEmails, catHipHopEmails, catReggaeEmails;
        List<String> catAfroPhones, catJazzPhones, catComedyPhones;
        List<String> catPoetryPhones, catHipHopPhones, catReggaePhones;

        List<String>       contactEmails;
        List<String>       contactPhones;
        List<List<String>> contactShows;

        // first name + surname per unique contact (for demographics)
        List<String> contactFirstNames;
        List<String> contactSurnames;

        List<Integer> showTicketCounts;
        int[] catTicketCounts;
        int   totalTickets;
        int[] timeBuckets;      // [Morning, Afternoon, Evening, Late]
        int[] groupSizeBuckets; // index 0 unused; 1-5 exact; 6 = 6+
    }

    // Demographics inference result
    private static class DemographicResult {
        int genderMale, genderFemale, genderUnknown;
        // Cultural/linguistic background estimate
        int bgBlackAfrican, bgAfrikaans, bgEnglish, bgAsian, bgOther, bgUnknown;
    }


    private static class GraphAnalysis {
        List<Set<Integer>> showToContacts;   // show index  → set of contact indices
        List<Set<Integer>> contactToShows;   // contact idx → set of show indices
        double[][]         jaccardMatrix;    // S×S Jaccard similarity
        int[]              showDegree;       // unique attendees per show
        List<List<Integer>> contactComponents; // BFS components (size > 1)
        int   crossGenreFans;
        int[] showBridgeness;
    }

    private static class StatResult {
        double pearsonR;
        String pearsonInterp;
        double chiSquared;
        double chiSquaredP;
        double cohortRetentionRate;
        List<Double> movingAvgRevenue;
        List<String> movingAvgLabels;
        double[] showZScores;
        List<String[]> topJaccardPairs; // [showA, showB, "0.xxx"]
    }

    private static class GreedyResult {
        static final double TARGET = 0.80;
        List<String>  coverSet;
        List<Integer> coverMarginal;
        int   totalCovered;
        int   totalContacts;
        double coveragePct;
        List<String>  catOrder;
        List<Integer> catMarginal;
    }

    private static class DPResult {
        List<String> optimalShows;
        int capacityUsed;
        int expectedAudience;
        List<String[]> lcsSimilarity; // [showA, showB, lcsLen]
    }

    private static class FinancialAnalysis {
        double totalRevenue;
        double[] categoryRevenue = new double[CATEGORIES.length];
        Map<String, Double> yearlyRevenue = new TreeMap<>();
        Map<Integer, Double> monthlyRevenue = new TreeMap<>();
        Map<String, Double> seasonalRevenue = new LinkedHashMap<>();
        int checkedIn;
        int totalRows;

        FinancialAnalysis() {
            seasonalRevenue.put("Summer", 0.0);
            seasonalRevenue.put("Autumn", 0.0);
            seasonalRevenue.put("Winter", 0.0);
            seasonalRevenue.put("Spring", 0.0);
        }
    }

    private static class UploadResult {
        int status;
        String fileName;
        String error;

        static UploadResult success(String fileName) {
            UploadResult result = new UploadResult();
            result.status = 201;
            result.fileName = fileName;
            return result;
        }

        static UploadResult failure(int status, String error) {
            UploadResult result = new UploadResult();
            result.status = status;
            result.error = error;
            return result;
        }
    }

    // ================================================================
    //  SERVER BOOTSTRAP
    // ================================================================

    public static void main(String[] args) throws Exception {
        int port = Integer.getInteger("ticketbuyer.port", 8080);
        if(port<1||port>65535)throw new IllegalArgumentException("ticketbuyer.port must be between 1 and 65535");
        HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
        server.createContext("/",                 TicketBuyerWebApp::handleReactApp);
        server.createContext("/showEmails",       TicketBuyerWebApp::handleReactApp);
        server.createContext("/showPhones",       TicketBuyerWebApp::handleReactApp);
        server.createContext("/categoryContacts", TicketBuyerWebApp::handleReactApp);
        server.createContext("/categoryPhones",   TicketBuyerWebApp::handleReactApp);
        server.createContext("/logo",             TicketBuyerWebApp::handleLogo);
        server.createContext("/upload",           TicketBuyerWebApp::handleUpload);
        server.createContext("/exclusions",       TicketBuyerWebApp::handleReactApp);
        server.createContext("/categories",       TicketBuyerWebApp::handleReactApp);
        server.createContext("/download",         TicketBuyerWebApp::handleDownload);
        server.createContext("/dataInsights",     TicketBuyerWebApp::handleReactApp);
        server.createContext("/report",            TicketBuyerWebApp::handleReactApp);
        server.createContext("/api/dashboard",    TicketBuyerWebApp::handleApiDashboard);
        server.createContext("/api/show",         TicketBuyerWebApp::handleApiShow);
        server.createContext("/api/category",     TicketBuyerWebApp::handleApiCategory);
        server.createContext("/api/insights",     TicketBuyerWebApp::handleApiInsights);
        server.createContext("/api/report",       TicketBuyerWebApp::handleApiReport);
        server.createContext("/api/settings",     TicketBuyerWebApp::handleApiSettings);
        server.createContext("/api/upload",       TicketBuyerWebApp::handleApiUpload);
        server.createContext("/api/health",       TicketBuyerWebApp::handleApiHealth);
        server.setExecutor(null);
        server.start();
        System.out.println("The One Room Database Manager → http://localhost:"+port+"/");
    }

    // ================================================================
    //  ALGORITHM ENGINE 1   GRAPH THEORY
    // ================================================================

    private static GraphAnalysis runGraphAnalysis(ProcessedData data,
                                                  List<String> ovShows,
                                                  List<String> ovCats) {
        int S = data.showNames.size();
        int C = data.contactEmails.size();
        GraphAnalysis g = new GraphAnalysis();

        // ── bipartite adjacency ──────────────────────────────────────
        g.showToContacts = new ArrayList<>();
        g.contactToShows = new ArrayList<>();
        g.showDegree     = new int[S];
        for (int i = 0; i < S; i++) g.showToContacts.add(new HashSet<>());
        for (int i = 0; i < C; i++) g.contactToShows.add(new HashSet<>());

        for (int ci = 0; ci < C; ci++) {
            List<String> attended = data.contactShows.get(ci);
            if (attended == null) continue;
            for (String sn : attended) {
                int si = indexOfString(data.showNames, sn);
                if (si >= 0) {
                    g.showToContacts.get(si).add(ci);
                    g.contactToShows.get(ci).add(si);
                    g.showDegree[si]++;
                }
            }
        }

        // ── Jaccard similarity matrix ────────────────────────────────
        g.jaccardMatrix = new double[S][S];
        for (int a = 0; a < S; a++) {
            g.jaccardMatrix[a][a] = 1.0;
            for (int b = a + 1; b < S; b++) {
                Set<Integer> A = g.showToContacts.get(a);
                Set<Integer> B = g.showToContacts.get(b);
                if (A.isEmpty() && B.isEmpty()) continue;
                int inter = 0;
                for (int x : A) if (B.contains(x)) inter++;
                int union = A.size() + B.size() - inter;
                double j = union == 0 ? 0 : (double) inter / union;
                g.jaccardMatrix[a][b] = g.jaccardMatrix[b][a] = j;
            }
        }

        // ── BFS connected components (contact projection) ────────────
        g.contactComponents = new ArrayList<>();
        boolean[] visited = new boolean[C];
        for (int start = 0; start < C; start++) {
            if (visited[start] || g.contactToShows.get(start).isEmpty()) continue;
            List<Integer> comp = new ArrayList<>();
            Queue<Integer> q = new LinkedList<>();
            q.add(start); visited[start] = true;
            while (!q.isEmpty()) {
                int ci = q.poll(); comp.add(ci);
                for (int si : g.contactToShows.get(ci)) {
                    for (int nb : g.showToContacts.get(si)) {
                        if (!visited[nb]) { visited[nb] = true; q.add(nb); }
                    }
                }
            }
            if (comp.size() > 1) g.contactComponents.add(comp);
        }

        // ── cross-genre fans ─────────────────────────────────────────
        g.crossGenreFans = 0;
        for (int ci = 0; ci < C; ci++) {
            List<String> att = data.contactShows.get(ci);
            if (att == null) continue;
            Set<String> cats = new HashSet<>();
            for (String sn : att) cats.add(categoryForShow(sn, ovShows, ovCats));
            if (cats.size() >= 2) g.crossGenreFans++;
        }

        // ── bridgeness proxy ─────────────────────────────────────────
        g.showBridgeness = new int[S];
        for (int ci = 0; ci < C; ci++) {
            List<Integer> sl = new ArrayList<>(g.contactToShows.get(ci));
            for (int i = 0; i < sl.size(); i++)
                for (int j = i + 1; j < sl.size(); j++) {
                    g.showBridgeness[sl.get(i)]++;
                    g.showBridgeness[sl.get(j)]++;
                }
        }
        return g;
    }

    // ================================================================
    //  ALGORITHM ENGINE 2   STATISTICS
    // ================================================================

    private static StatResult runStatistics(ProcessedData data,
                                            GraphAnalysis graph,
                                            File folder) {
        StatResult r = new StatResult();
        int S = data.showNames.size();

        // ── Pearson r: ticket count vs unique-contact count ──────────
        if (S >= 3) {
            double[] x = new double[S], y = new double[S];
            for (int i = 0; i < S; i++) {
                x[i] = data.showTicketCounts.get(i);
                y[i] = graph.showToContacts.get(i).size();
            }
            r.pearsonR     = pearson(x, y);
            r.pearsonInterp = interpretPearson(r.pearsonR);
        } else {
            r.pearsonR = Double.NaN;
            r.pearsonInterp = "need ≥3 shows";
        }

        // ── chi-squared: booking time-of-day vs uniform (df=3) ───────
        {
            int total = 0;
            for (int v : data.timeBuckets) total += v;
            double expected = total / 4.0;
            double chi2 = 0;
            for (int v : data.timeBuckets) {
                double d = v - expected;
                chi2 += d * d / Math.max(expected, 1);
            }
            r.chiSquared  = chi2;
            r.chiSquaredP = chiSquaredPDf3(chi2);
        }

        // ── cohort retention ─────────────────────────────────────────
        int returners = 0, total = 0;
        for (List<String> shows : data.contactShows) {
            if (shows == null || shows.isEmpty()) continue;
            total++;
            if (shows.size() > 1) returners++;
        }
        r.cohortRetentionRate = total == 0 ? 0 : returners * 100.0 / total;

        // ── 3-period revenue moving average ──────────────────────────
        r.movingAvgRevenue = new ArrayList<>();
        r.movingAvgLabels  = new ArrayList<>();
        Map<String, Double> yearRev = new LinkedHashMap<>();
        File[] csvs = folder.listFiles((d, n) -> n.toLowerCase().endsWith(".csv")
                && !n.equalsIgnoreCase("contacts_with_shows.csv")
                && !n.equalsIgnoreCase("all_contacts.csv"));
        if (csvs == null) csvs = new File[0];
        Arrays.sort(csvs, (a, b) -> a.getName().compareToIgnoreCase(b.getName()));
        for (File f : csvs) {
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8))) {
                String header = br.readLine(); if (header == null) continue;
                char delimiter = detectDelimiter(header);
                List<String> cols = parseCsvLine(header, delimiter);
                int iP = findIndex(cols, "Price paid"), iD = findIndex(cols, "Purchase Date");
                String line;
                while ((line = br.readLine()) != null) {
                    List<String> row = parseCsvLine(line, delimiter);
                    double price = iP >= 0 && iP < row.size() ? parsePrice(row.get(iP)) : 0;
                    if (iD >= 0 && iD < row.size()) {
                        String pd = row.get(iD);
                        if (pd != null && pd.length() >= 4)
                            yearRev.merge(pd.substring(0, 4), price, Double::sum);
                    }
                }
            } catch (Exception ignored) {}
        }
        List<String> yrKeys = new ArrayList<>(yearRev.keySet());
        Collections.sort(yrKeys);
        List<Double> yrVals = new ArrayList<>();
        for (String k : yrKeys) yrVals.add(yearRev.get(k));
        for (int i = 2; i < yrVals.size(); i++) {
            r.movingAvgRevenue.add((yrVals.get(i-2) + yrVals.get(i-1) + yrVals.get(i)) / 3.0);
            r.movingAvgLabels.add(yrKeys.get(i));
        }

        // ── z-scores on show ticket volumes ──────────────────────────
        r.showZScores = new double[S];
        if (S > 1) {
            double mean = 0;
            for (int i = 0; i < S; i++) mean += data.showTicketCounts.get(i);
            mean /= S;
            double var = 0;
            for (int i = 0; i < S; i++) { double d = data.showTicketCounts.get(i) - mean; var += d*d; }
            double std = Math.sqrt(var / S);
            for (int i = 0; i < S; i++)
                r.showZScores[i] = std == 0 ? 0 : (data.showTicketCounts.get(i) - mean) / std;
        }

        // ── top Jaccard pairs ─────────────────────────────────────────
        r.topJaccardPairs = new ArrayList<>();
        List<double[]> pairs = new ArrayList<>();
        for (int a = 0; a < S; a++)
            for (int b = a+1; b < S; b++)
                pairs.add(new double[]{a, b, graph.jaccardMatrix[a][b]});
        pairs.sort((p, q) -> Double.compare(q[2], p[2]));
        for (int i = 0; i < Math.min(6, pairs.size()); i++) {
            double[] p = pairs.get(i);
            r.topJaccardPairs.add(new String[]{
                    data.showNames.get((int) p[0]),
                    data.showNames.get((int) p[1]),
                    String.format(Locale.ROOT, "%.3f", p[2])
            });
        }
        return r;
    }

    // ================================================================
    //  ALGORITHM ENGINE 3   GREEDY (Set Cover)
    // ================================================================

    /**
     * Classic greedy set-cover approximation.
     * At each step, select the show that covers the most uncovered contacts.
     * Approximation ratio: ln(n) + 1   where n = |universe|
     * Time complexity: O(S × C) per step, O(S² × C) total worst case.
     */
    private static GreedyResult runGreedy(ProcessedData data, GraphAnalysis graph,
                                          List<String> ovShows, List<String> ovCats) {
        GreedyResult g = new GreedyResult();
        int S = data.showNames.size();
        int C = data.contactEmails.size();
        g.totalContacts = C;
        g.coverSet      = new ArrayList<>();
        g.coverMarginal = new ArrayList<>();

        Set<Integer> covered   = new HashSet<>();
        Set<Integer> remaining = new LinkedHashSet<>();
        for (int s = 0; s < S; s++) remaining.add(s);
        int target = (int) Math.ceil(C * GreedyResult.TARGET);

        while (covered.size() < target && !remaining.isEmpty()) {
            int bestShow = -1, bestGain = 0;
            for (int s : remaining) {
                int gain = 0;
                for (int ci : graph.showToContacts.get(s)) if (!covered.contains(ci)) gain++;
                if (gain > bestGain) { bestGain = gain; bestShow = s; }
            }
            if (bestShow < 0 || bestGain == 0) break;
            covered.addAll(graph.showToContacts.get(bestShow));
            g.coverSet.add(data.showNames.get(bestShow));
            g.coverMarginal.add(bestGain);
            remaining.remove(bestShow);
        }
        g.totalCovered = covered.size();
        g.coveragePct  = C == 0 ? 0 : covered.size() * 100.0 / C;

        // ── greedy category reach order ───────────────────────────────
        g.catOrder    = new ArrayList<>();
        g.catMarginal = new ArrayList<>();
        Map<String, Set<Integer>> catMap = new LinkedHashMap<>();
        for (String cat : CATEGORIES) catMap.put(cat, new HashSet<>());
        for (int ci = 0; ci < C; ci++) {
            List<String> att = data.contactShows.get(ci);
            if (att == null) continue;
            for (String sn : att) catMap.get(categoryForShow(sn, ovShows, ovCats)).add(ci);
        }
        Set<Integer> catCovered  = new HashSet<>();
        Set<String>  catRemaining = new LinkedHashSet<>(Arrays.asList(CATEGORIES));
        while (!catRemaining.isEmpty()) {
            String bestCat = null; int bestGain = 0;
            for (String cat : catRemaining) {
                int gain = 0;
                for (int ci : catMap.get(cat)) if (!catCovered.contains(ci)) gain++;
                if (gain > bestGain) { bestGain = gain; bestCat = cat; }
            }
            if (bestCat == null || bestGain == 0) break;
            catCovered.addAll(catMap.get(bestCat));
            g.catOrder.add(bestCat); g.catMarginal.add(bestGain);
            catRemaining.remove(bestCat);
        }
        return g;
    }

    // ================================================================
    //  ALGORITHM ENGINE 4   DYNAMIC PROGRAMMING
    // ================================================================

    /**
     * 0/1 Knapsack: maximise unique audience within capacity = floor(S/2) slots.
     * Each show has weight = 1, value = unique-attendee count.
     * dp[i][w] = best audience reachable using first i shows, w slots.
     * Time: O(S × W)  |  Space: O(S × W)
     *
     * LCS audience similarity:
     * Treat each show's sorted contact-ID list as a sequence.
     * LCS length = longest common subsequence = shared-audience depth.
     * Time: O(m × n) per pair (capped at 200 elements for performance).
     */
    private static DPResult runDP(ProcessedData data, GraphAnalysis graph) {
        DPResult r = new DPResult();
        int S = data.showNames.size();
        int W = Math.max(1, S / 2);          // slot budget = half the shows
        int[] vals = new int[S];
        for (int i = 0; i < S; i++) vals[i] = graph.showToContacts.get(i).size();

        // knapsack table
        int[][] dp = new int[S + 1][W + 1];
        for (int i = 1; i <= S; i++)
            for (int w = 0; w <= W; w++) {
                dp[i][w] = dp[i-1][w];
                if (w >= 1) dp[i][w] = Math.max(dp[i][w], dp[i-1][w-1] + vals[i-1]);
            }

        // backtrack
        r.optimalShows     = new ArrayList<>();
        r.expectedAudience = dp[S][W];
        r.capacityUsed     = 0;
        int w = W;
        for (int i = S; i >= 1; i--) {
            if (dp[i][w] != dp[i-1][w]) {
                r.optimalShows.add(data.showNames.get(i - 1));
                r.capacityUsed++; w--;
            }
        }
        Collections.reverse(r.optimalShows);

        // LCS pairs
        r.lcsSimilarity = new ArrayList<>();
        List<double[]> pairs = new ArrayList<>();
        for (int a = 0; a < S; a++) {
            for (int b = a+1; b < S; b++) {
                List<Integer> seqA = new ArrayList<>(graph.showToContacts.get(a));
                List<Integer> seqB = new ArrayList<>(graph.showToContacts.get(b));
                Collections.sort(seqA); Collections.sort(seqB);
                pairs.add(new double[]{a, b, lcs(seqA, seqB)});
            }
        }
        pairs.sort((p, q) -> Double.compare(q[2], p[2]));
        for (int i = 0; i < Math.min(5, pairs.size()); i++) {
            double[] p = pairs.get(i);
            r.lcsSimilarity.add(new String[]{
                    data.showNames.get((int) p[0]),
                    data.showNames.get((int) p[1]),
                    String.valueOf((int) p[2])
            });
        }
        return r;
    }

    // ================================================================
    //  ALGORITHM ENGINE 5   DEMOGRAPHICS
    // ================================================================

    /**
     * Infers gender from first names and cultural/linguistic background from surnames.
     * All results are probabilistic estimates based on South African naming traditions.
     */
    private static DemographicResult runDemographics(ProcessedData data) {
        DemographicResult r = new DemographicResult();

        Set<String> female = new HashSet<>(Arrays.asList(
                "zintle","noli","asenathi","lihle","somila","liyabona","yolisa","lilitha","sibongile",
                "zoleka","thizwilondi","comfort","uviwe","poppy","melissa","olilitha","chantel","nolipicane",
                "thandolwethu","siyasamkela","ncebakazi","nompumelelo","nomvula","thandi","nandi","bongiwe",
                "nokwanda","siwe","ayanda","zanele","nokuthula","ntombi","fikile","zodwa","lungile","buhle",
                "sanele","duduzile","nomsa","nthabiseng","mokgadi","mmapula","maite","kgomotso","kefilwe",
                "gaone","mmatlou","lerato","refilwe","palesa","kelebogile","boitumelo","mpho","dineo","tebogo",
                "sarah","jessica","emily","emma","olivia","ava","isabella","mia","sophia","charlotte",
                "amelia","harper","evelyn","abigail","ella","elizabeth","camila","luna","nora","lily",
                "eleanor","hannah","lillian","addison","aubrey","claire","samantha","grace","zoe","anna",
                "natalie","layla","brooklyn","savannah","aaliyah","audrey","skylar","paisley","maya",
                "alexa","ariana","elena","caroline","michelle","nicole","lisa","jennifer","ashley","amanda",
                "stephanie","patricia","linda","barbara","margaret","sandra","donna","carol","ruth","sharon",
                "helen","deborah","angela","cheryl","rieka","rikalet","elzette","aneke","liezel","marelize",
                "charlene","francine","elrina","mariette","hermien","ansie","riana","elsa","alida","anri",
                "rika","mei","xiu","ying","fang","yan","hui","jing","na","pin","shan","xia","yun","zhen",
                "aiko","emi","hana","keiko","sakura","naomi","yoko","akiko","priya","divya","ananya",
                "pooja","sneha","kavya","meera","lakshmi","sunita","geeta","masego","emkay"
        ));

        Set<String> male = new HashSet<>(Arrays.asList(
                "mbongisi","mlamli","avuyile","sivuyile","alasdair","ntsika","mxolisi","sandile",
                "lungelo","siphiwe","thabo","sipho","mandla","sibusiso","bongani","siyanda","ayabonga",
                "lwazi","lusanda","luzuko","luyanda","anele","andile","mlungisi","vusi","themba",
                "mthokozisi","kagiso","katlego","tshepang","karabo","modise","molefe","onthatile",
                "gosiame","phenyo","thapelo","boitshoko","itumeleng","tshepo","reitumetse","mojalefa",
                "lesego","tshiamo","mathabo","realeboga","james","john","robert","michael","william",
                "david","richard","joseph","thomas","charles","christopher","daniel","matthew","anthony",
                "donald","mark","paul","steven","andrew","kenneth","george","joshua","kevin","brian",
                "edward","ronald","timothy","jason","jeffrey","ryan","jacob","gary","nicholas","eric",
                "jonathan","stephen","larry","justin","scott","brandon","benjamin","samuel","raymond",
                "frank","gregory","patrick","alexander","jack","dennis","jerry","tyler","aaron","henry",
                "douglas","peter","zachary","kyle","walter","harold","jeremy","ethan","carl","keith",
                "roger","gerald","christian","terry","sean","arthur","noah","adam","austin","dylan",
                "billy","bruce","russell","wayne","roy","louis","alan","haiyang","pieter","jan","hendrik",
                "christiaan","francois","gert","nico","danie","hannes","kobus","louw","marthinus",
                "neels","schalk","wikus","thys","wei","jian","hao","ming","jun","feng","bo","lei",
                "qiang","zhi","tao","rong","hiroshi","kenji","takeshi","raj","rahul","arjun","amit","vijay"
        ));

        Set<String> africanSurnPfx = new HashSet<>(Arrays.asList(
                "ngo","nkh","ndl","mth","mhl","mng","mny","mzo","zwe","sib","sip","siy",
                "dub","dwy","bul","bon","nts","nto","ntl","mko","mke","mfi","mza","luy",
                "lus","zem","xhi","xol","ses","suk","dya","buk","gcu","gqa","gxe","gci",
                "nxa","hla","mpi","mpu","mpo","sob","moj","dlo","gam","gal"
        ));
        Set<String> africanSurns = new HashSet<>(Arrays.asList(
                "mnyaka","dywili","picane","feni","mathanjana","mhlangabezo","mkhonza","dyantyi",
                "tyhomfi","xhinti","dlamini","bulu","zembeta","binjana","ngcana","ntshaluba",
                "ngxabela","ramakuela","mfiniza","bonke","sulo","negge","gotyana","adam","dumezweni"
        ));

        int C = data.contactFirstNames.size();
        for (int i = 0; i < C; i++) {
            String fn = data.contactFirstNames.get(i).toLowerCase().trim();
            String sn = data.contactSurnames.get(i).toLowerCase().trim();

            if (female.contains(fn))    r.genderFemale++;
            else if (male.contains(fn)) r.genderMale++;
            else                        r.genderUnknown++;

            if (sn.equals("yao")||fn.equals("haiyang")||fn.equals("mei")||fn.equals("xiu")) {
                r.bgAsian++;
            } else if (sn.startsWith("de ")||sn.startsWith("van ")||sn.startsWith("du ")||sn.startsWith("le ")
                    ||sn.equals("snyman")||fn.equals("rikalet")||sn.endsWith("berg")||sn.endsWith("bosch")) {
                r.bgAfrikaans++;
            } else if (sn.equals("gillies")||sn.startsWith("mc")||sn.startsWith("mac")) {
                r.bgEnglish++;
            } else if (africanSurns.contains(sn)) {
                r.bgBlackAfrican++;
            } else {
                boolean matched = false;
                for (String p : africanSurnPfx) if (sn.startsWith(p)) { matched = true; break; }
                if (matched) r.bgBlackAfrican++;
                else         r.bgUnknown++;
            }
        }
        return r;
    }

    // ================================================================
    //  MATH UTILITIES
    // ================================================================

    private static double pearson(double[] x, double[] y) {
        int n = x.length;
        double mx = 0, my = 0;
        for (int i = 0; i < n; i++) { mx += x[i]; my += y[i]; }
        mx /= n; my /= n;
        double num = 0, dx2 = 0, dy2 = 0;
        for (int i = 0; i < n; i++) {
            double dx = x[i]-mx, dy = y[i]-my;
            num += dx*dy; dx2 += dx*dx; dy2 += dy*dy;
        }
        double denom = Math.sqrt(dx2 * dy2);
        return denom == 0 ? 0 : num / denom;
    }

    /** chi-squared p-value approximation, df = 3 */
    private static double chiSquaredPDf3(double c) {
        if (c < 0.352)  return 0.95;
        if (c < 0.584)  return 0.90;
        if (c < 1.213)  return 0.75;
        if (c < 2.366)  return 0.50;
        if (c < 4.108)  return 0.25;
        if (c < 6.251)  return 0.10;
        if (c < 7.815)  return 0.05;
        if (c < 9.348)  return 0.025;
        if (c < 11.345) return 0.01;
        return 0.001;
    }

    /** LCS length via DP, capped at 200 elements per sequence for performance */
    private static int lcs(List<Integer> a, List<Integer> b) {
        int m = Math.min(a.size(), 200);
        int n = Math.min(b.size(), 200);
        int[][] dp = new int[m+1][n+1];
        for (int i = 1; i <= m; i++)
            for (int j = 1; j <= n; j++)
                dp[i][j] = a.get(i-1).equals(b.get(j-1))
                        ? dp[i-1][j-1] + 1
                        : Math.max(dp[i-1][j], dp[i][j-1]);
        return dp[m][n];
    }

    private static String interpretPearson(double r) {
        double a = Math.abs(r);
        String dir = r >= 0 ? "positive" : "negative";
        if (a >= 0.9) return "very strong " + dir;
        if (a >= 0.7) return "strong " + dir;
        if (a >= 0.5) return "moderate " + dir;
        if (a >= 0.3) return "weak " + dir;
        return "negligible correlation";
    }

    // ================================================================
    //  DATA INSIGHTS PAGE  (main analytics view + all 4 algo sections)
    // ================================================================

    private static void handleDataInsights(HttpExchange ex) throws IOException {
        File folder = new File(".");
        ProcessedData data = processFolder(folder);

        List<String> ovShows = new ArrayList<>(), ovCats = new ArrayList<>();
        loadCategoryOverrides(folder, ovShows, ovCats);

        // run all engines
        GraphAnalysis    graph  = runGraphAnalysis(data, ovShows, ovCats);
        StatResult       stats  = runStatistics(data, graph, folder);
        GreedyResult     greedy = runGreedy(data, graph, ovShows, ovCats);
        DPResult         dp     = runDP(data, graph);
        DemographicResult demo  = runDemographics(data);

        int S = data.showNames.size();
        int C = data.contactEmails.size();

        // ── legacy revenue aggregation ────────────────────────────────
        File[] csvs = folder.listFiles((d, n) -> n.toLowerCase().endsWith(".csv")
                && !n.equalsIgnoreCase("contacts_with_shows.csv")
                && !n.equalsIgnoreCase("all_contacts.csv"));
        if (csvs == null) csvs = new File[0];
        Arrays.sort(csvs, (a, b) -> a.getName().compareToIgnoreCase(b.getName()));

        double totalRevenue = 0;
        double[] catRevenue = new double[6];
        Map<String, Double> yearRevMap   = new LinkedHashMap<>();
        Map<String, Double> monthRevMap  = new LinkedHashMap<>();
        Map<String, Double> seasonRevMap = new LinkedHashMap<>();
        int checkedIn = 0, totalRows = 0;

        for (File f : csvs) {
            String base = f.getName().replace(".csv","");
            int ci = categoryIndex(categoryForShow(base, ovShows, ovCats)) - 1;
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8))) {
                String header = br.readLine(); if (header == null) continue;
                char delimiter = detectDelimiter(header);
                List<String> cols = parseCsvLine(header, delimiter);
                int iPrice=findIndex(cols,"Price paid"), iDate=findIndex(cols,"Purchase Date"),
                        iCheck=findIndex(cols,"Checked In"), iEvent=findIndex(cols,"EventDate");
                String line;
                while ((line = br.readLine()) != null) {
                    List<String> row = parseCsvLine(line, delimiter);
                    double price = iPrice>=0&&iPrice<row.size() ? parsePrice(row.get(iPrice)) : 0;
                    if (price > 0) { totalRevenue += price; if(ci>=0&&ci<6) catRevenue[ci]+=price; }
                    if (iDate>=0&&iDate<row.size()) {
                        String pd = row.get(iDate);
                        if (pd!=null&&pd.length()>=4) yearRevMap.merge(pd.substring(0,4), price, Double::sum);
                    }
                    try {
                        if (iEvent>=0&&iEvent<row.size()) {
                            String ed = row.get(iEvent);
                            if (ed!=null&&!ed.isEmpty()) {
                                LocalDate ev = LocalDate.parse(ed.split(" ")[0]);
                                monthRevMap.merge(ev.getMonth().toString(), price, Double::sum);
                                int m = ev.getMonthValue();
                                String se = (m==12||m==1||m==2)?"Summer":m<=5?"Autumn":m<=8?"Winter":"Spring";
                                seasonRevMap.merge(se, price, Double::sum);
                            }
                        }
                    } catch (Exception ignored) {}
                    if (iCheck>=0&&iCheck<row.size()&&"Yes".equalsIgnoreCase(row.get(iCheck))) checkedIn++;
                    totalRows++;
                }
            } catch (Exception ignored) {}
        }

        double attendanceRate = totalRows==0?0:checkedIn*100.0/totalRows;
        List<String> years = new ArrayList<>(yearRevMap.keySet()); Collections.sort(years);
        double[] yearRevArr = new double[years.size()];
        for (int i=0;i<years.size();i++) yearRevArr[i]=yearRevMap.get(years.get(i));
        String bestYear="-"; double bestYearVal=0;
        for(int i=0;i<years.size();i++) if(yearRevArr[i]>bestYearVal){bestYearVal=yearRevArr[i];bestYear=years.get(i);}
        String bestMonth="-",worstMonth="-"; double bestMonthVal=0,worstMonthVal=Double.MAX_VALUE;
        for(Map.Entry<String,Double> e:monthRevMap.entrySet()){if(e.getValue()>bestMonthVal){bestMonthVal=e.getValue();bestMonth=e.getKey();}if(e.getValue()<worstMonthVal){worstMonthVal=e.getValue();worstMonth=e.getKey();}}
        if(worstMonthVal==Double.MAX_VALUE)worstMonthVal=0;
        String bestSeason="-"; double bestSeasonVal=0;
        for(Map.Entry<String,Double> e:seasonRevMap.entrySet()) if(e.getValue()>bestSeasonVal){bestSeasonVal=e.getValue();bestSeason=e.getKey();}
        String[] catNames = {"Afrosoul","Jazz","Comedy","Folk","HipHop","Reggae"};
        int bestCatIdx=0; double bestCatVal=0;
        for(int i=0;i<catRevenue.length;i++) if(catRevenue[i]>bestCatVal){bestCatVal=catRevenue[i];bestCatIdx=i;}
        String[] timeLabels={"Morning","Afternoon","Evening","Late"};
        int maxT=0; for(int i=1;i<data.timeBuckets.length;i++) if(data.timeBuckets[i]>data.timeBuckets[maxT]) maxT=i;
        int newC=0,retC=0; for(List<String>sh:data.contactShows){int v=sh==null?0:sh.size();if(v<=1)newC++;else retC++;}
        double newRate=C==0?0:newC*100.0/C, retRate=C==0?0:retC*100.0/C;
        double avgSh=C==0?0:(double)(newC+retC*2)/C; // rough proxy

        // ── HTML ──────────────────────────────────────────────────────
        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html><html><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width,initial-scale=1'>");
        html.append("<title>Data Insights · The One Room</title>");
        html.append(insightsCss());
        html.append("<script src='https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js'></script>");
        html.append("</head><body><div class='wrapper'>");
        html.append("<nav class='utility-nav' aria-label='Application navigation'><a href='/'>&#8592; Dashboard</a><div class='utility-nav-group'><a href='/report'>Reports</a><a href='/exclusions'>Settings</a></div></nav>");

        // ── HERO ──────────────────────────────────────────────────────
        html.append("<div class='hero'>");
        html.append("<div class='hero-pill'>The One Room · Audience console</div>");
        html.append("<h1 class='hero-title'>Data Insights</h1>");
        html.append("<p class='hero-sub'>A complete picture of your revenue, your audience, and how they behave   from ticket sales to booking patterns.</p>");
        html.append("<p class='hero-meta'>").append(S).append(" shows &nbsp;·&nbsp; ")
                .append(C).append(" unique contacts &nbsp;·&nbsp; ")
                .append(data.totalTickets).append(" total tickets</p>");
        html.append("</div>");

        // ── SECTION NAV ───────────────────────────────────────────────
        html.append("<nav class='section-nav'>");
        html.append("<a href='#revenue'>Revenue &amp; Audience</a>");
        html.append("<a href='#graph'>Audience Connections</a>");
        html.append("<a href='#stats'>Patterns &amp; Trends</a>");
        html.append("<a href='#greedy'>Reach Optimisation</a>");
        html.append("<a href='#dp'>Schedule Planning</a>");
        html.append("<a href='#demographics'>Audience Demographics</a>");
        html.append("</nav>");


        // ================================================================
        //  SECTION 1   REVENUE
        // ================================================================
        html.append("<section id='revenue'>");
        html.append("<h2 class='section-heading'>Revenue &amp; Audience</h2>");
        html.append("<p class='section-desc'>These numbers give you a quick read on the financial health and audience makeup of The One Room. Every figure is drawn directly from your Webtickets CSV files.</p>");
        html.append("<hr class='section-divider'>");

        // KPI strip
        html.append("<div class='kpi-row'>");
        kpi(html, formatCurrency(totalRevenue), "Total Revenue");
        kpi(html, String.format(Locale.ROOT, "%.1f%%", retRate),  "Returning Audience");
        kpi(html, String.format(Locale.ROOT, "%.1f%%", newRate),  "New Audience");
        kpi(html, String.format(Locale.ROOT, "%.1f%%", attendanceRate), "Attendance Rate");
        kpi(html, String.format(Locale.ROOT, "%.2f",  (double)(newC+retC*2)/Math.max(C,1)), "Avg Shows / Person");
        html.append("</div>");

        // charts   using extended chartCard with explanation text
        html.append("<div class='charts-grid'>");

        html.append("<div class='chart-card'><div class='chart-card-title'>Revenue over time</div>");
        html.append("<p class='chart-explanation'>How much money came in each year. A rising line means the club is growing. A dip is worth investigating   was a year light on shows, or did a particular genre underperform?</p>");
        html.append("<div class='chart-wrap'><canvas id='yearChart'></canvas></div>");
        if(!bestYear.equals("-")) html.append("<p class='chart-insight'>Your strongest year on record was <strong>").append(bestYear).append("</strong>, bringing in ").append(formatCurrency(bestYearVal)).append(".</p>");
        html.append("</div>");

        html.append("<div class='chart-card'><div class='chart-card-title'>Revenue by genre</div>");
        html.append("<p class='chart-explanation'>Which music or comedy genre has generated the most revenue in total. Taller bars mean more money earned. This tells you where to invest more   or where there may be untapped potential.</p>");
        html.append("<div class='chart-wrap'><canvas id='catChart'></canvas></div>");
        html.append("<p class='chart-insight'>The highest-earning genre to date is <strong>").append(catNames[bestCatIdx]).append("</strong> at ").append(formatCurrency(bestCatVal)).append(".</p>");
        html.append("</div>");

        html.append("<div class='chart-card'><div class='chart-card-title'>New vs returning audience</div>");
        html.append("<p class='chart-explanation'>Are people coming back? The gold slice represents people who have attended more than one show. The teal slice represents first-timers. A healthy club has a growing returning slice   it means people liked what they saw.</p>");
        html.append("<div class='chart-wrap'><canvas id='retentionChart'></canvas></div>");
        html.append("<p class='chart-insight'>").append(String.format(Locale.ROOT, "%.1f%%",retRate)).append(" of your audience has come back for a second show or more.</p>");
        html.append("</div>");

        html.append("<div class='chart-card'><div class='chart-card-title'>When people book tickets</div>");
        html.append("<p class='chart-explanation'>The time of day when tickets are most often purchased. This tells you the best window to send marketing emails or post on social media   because that is when your audience is already in buying mode.</p>");
        html.append("<div class='chart-wrap'><canvas id='timeChart'></canvas></div>");
        html.append("<p class='chart-insight'>Most bookings happen in the <strong>").append(timeLabels[maxT]).append("</strong>. Send your campaigns then for the best chance of being seen.</p>");
        html.append("</div>");

        // compute most common group size for the insight line
        int topGroupIdx = 1;
        for (int gi = 2; gi < data.groupSizeBuckets.length; gi++)
            if (data.groupSizeBuckets[gi] > data.groupSizeBuckets[topGroupIdx]) topGroupIdx = gi;
        String topGroupLabel = topGroupIdx == 6 ? "6 or more" : String.valueOf(topGroupIdx);
        String topGroupDesc  = topGroupIdx == 1 ? "people typically buy on their own"
                : topGroupIdx == 2 ? "most orders are for two people   the majority of your audience comes as a pair"
                  : "the most common order is for " + topGroupLabel + " tickets, which suggests people are coming in groups and bringing friends";

        html.append("<div class='chart-card'><div class='chart-card-title'>How many tickets people buy at once</div>");
        html.append("<p class='chart-explanation'>Each bar represents how many orders were placed for that number of tickets   1 ticket, 2 tickets, and so on. This tells you whether your audience tends to come alone, as a couple, or in groups. Large group orders are a strong sign of word-of-mouth: people are actively inviting others.</p>");
        html.append("<div class='chart-wrap'><canvas id='groupChart'></canvas></div>");
        html.append("<p class='chart-insight'>Based on your data, ").append(topGroupDesc).append(". Consider a group discount for 4 or more if you want to actively encourage larger bookings and grow word-of-mouth.</p>");
        html.append("</div>");

        html.append("<div class='chart-card'><div class='chart-card-title'>Revenue by season</div>");
        html.append("<p class='chart-explanation'>Which time of year brings in the most money. South African seasons are used   Summer (Dec Feb), Autumn (Mar May), Winter (Jun Aug), Spring (Sep Nov). Use this to plan when to schedule your biggest shows.</p>");
        html.append("<div class='chart-wrap'><canvas id='seasonChart'></canvas></div>");
        if(!bestMonth.equals("-")) html.append("<p class='chart-insight'>Your peak month is <strong>").append(bestMonth).append("</strong>. Your strongest season overall is <strong>").append(bestSeason).append("</strong>.</p>");
        html.append("</div>");

        html.append("</div>"); // charts-grid
        html.append("</section>");

        // ================================================================
        //  SECTION 2   AUDIENCE CONNECTIONS
        // ================================================================
        html.append("<section id='graph'>");
        html.append("<h2 class='section-heading'>Audience Connections</h2>");
        html.append("<p class='section-desc'>This section looks at your audience as a network   who goes to which shows, which shows share the same fans, and which shows act as bridges between different audience groups. Think of it like mapping overlapping friend circles across your events.</p>");
        html.append("<hr class='section-divider'>");

        html.append("<div class='algo-grid'>");

        // graph KPI card
        html.append("<div class='algo-card'><div class='algo-card-title'>Audience network at a glance</div>");
        html.append("<p class='algo-explain'>These figures describe how your overall audience is connected across shows.</p>");
        int maxBridgeIdx=0;
        for(int i=1;i<graph.showBridgeness.length;i++) if(graph.showBridgeness[i]>graph.showBridgeness[maxBridgeIdx]) maxBridgeIdx=i;
        statRow(html,"Total ticket connections across all shows",String.valueOf(data.totalTickets),false);
        statRow(html,"Distinct audience clusters identified",String.valueOf(graph.contactComponents.size()),false);
        statRow(html,"Fans who attend more than one genre",String.valueOf(graph.crossGenreFans),true);
        if(S>0){
            statRow(html,"Show that bridges the most audience groups",escapeHtml(data.showNames.get(maxBridgeIdx)),false);
        }
        html.append("<p class='algo-insight'>Cross-genre fans are your most loyal and adventurous audience members. They are the best people to target when launching a new genre or a show outside their usual lane   they have already shown they are open to it.</p>");
        html.append("</div>");

        // Jaccard top pairs
        html.append("<div class='algo-card'><div class='algo-card-title'>Shows with the most shared fans</div>");
        html.append("<p class='algo-explain'>This table ranks show pairs by how much of their audience they share. A score of 1.0 would mean both shows had exactly the same fans. A score of 0.0 means no overlap at all. Higher scores tell you which shows appeal to the same people   useful for cross-promotion or scheduling shows back-to-back.</p>");
        if(stats.topJaccardPairs.isEmpty()){
            html.append("<p class='muted'>Add more shows to see audience overlap comparisons.</p>");
        } else {
            html.append("<table><thead><tr><th>Show A</th><th>Show B</th><th>Overlap score</th><th></th></tr></thead><tbody>");
            for(String[] pair:stats.topJaccardPairs){
                int pct=(int)(Double.parseDouble(pair[2])*100);
                html.append("<tr><td>").append(escapeHtml(pair[0])).append("</td>")
                        .append("<td>").append(escapeHtml(pair[1])).append("</td>")
                        .append("<td><strong class='gold'>").append(pair[2]).append("</strong></td>")
                        .append("<td><div class='minibar'><div class='minibar-fill' style='width:").append(pct).append("%'></div></div></td></tr>");
            }
            html.append("</tbody></table>");
        }
        html.append("<p class='algo-insight'>If two shows share a large portion of their audience, consider emailing fans of one show when the other goes on sale. They are very likely to buy.</p>");
        html.append("</div>");

        // degree centrality chart
        html.append("<div class='algo-card wide'><div class='algo-card-title'>Unique audience size per show</div>");
        html.append("<p class='algo-explain'>Each bar shows how many different people attended that show   not tickets sold, but unique individuals. A show can sell many tickets but still have a small unique audience if the same people keep coming back. Taller bars mean broader reach into your wider audience base.</p>");
        html.append("<div class='chart-wrap'><canvas id='degreeChart'></canvas></div>");
        html.append("<p class='algo-insight'>Shows with a large unique audience are your best vehicles for growing the database. Shows with a smaller unique audience may have a loyal but narrow fan base   valuable for retention, but less effective for new contact acquisition.</p>");
        html.append("</div>");

        // bridgeness chart
        html.append("<div class='algo-card wide'><div class='algo-card-title'>Which shows connect the most audience groups</div>");
        html.append("<p class='algo-explain'>This measures how many different audience pairs a show brings together   in other words, which shows attract people who also attend a wide variety of other shows. A high score means that show is a hub: its audience fans out across the rest of the programme. These are your most strategically valuable shows for cross-promotion.</p>");
        html.append("<div class='chart-wrap'><canvas id='bridgeChart'></canvas></div>");
        html.append("<p class='algo-insight'>The show with the highest bar is the one whose audience has the most connections to other shows. When that show goes on sale, promoting other upcoming events alongside it will have the widest reach.</p>");
        html.append("</div>");

        html.append("</div>"); // algo-grid
        html.append("</section>");

        // ================================================================
        //  SECTION 3   PATTERNS AND TRENDS
        // ================================================================
        html.append("<section id='stats'>");
        html.append("<h2 class='section-heading'>Patterns &amp; Trends</h2>");
        html.append("<p class='section-desc'>This section uses statistical methods to find patterns in your data that are not obvious from looking at individual numbers. It tells you which behaviours are meaningful and which might just be noise.</p>");
        html.append("<hr class='section-divider'>");

        html.append("<div class='algo-grid'>");

        // Pearson + chi2 card
        html.append("<div class='algo-card'><div class='algo-card-title'>Are bigger shows reaching more people?</div>");
        html.append("<p class='algo-explain'>This measures whether the shows that sell more tickets also tend to reach more unique people. A strong positive result means your high-selling shows are genuinely broadening your audience. A weak result suggests that your busiest shows may be driven by the same loyal fans buying multiple tickets rather than new people walking in.</p>");
        if(!Double.isNaN(stats.pearsonR)){
            statRow(html,"Correlation score ( 1 to +1)",String.format(Locale.ROOT, "%.2f",stats.pearsonR),true);
            statRow(html,"What this means",stats.pearsonInterp,false);
        } else {
            html.append("<p class='muted'>Add at least 3 shows to calculate this.</p>");
        }
        html.append("<p class='algo-insight'>A score above 0.7 is strong   your best-selling shows are also your best audience-builders. A score below 0.3 suggests selling more tickets is not translating to a wider audience, and it may be worth experimenting with how you promote new shows to new people.</p>");
        html.append("</div>");

        // booking time significance card
        html.append("<div class='algo-card'><div class='algo-card-title'>Is there a genuine preferred booking time?</div>");
        html.append("<p class='algo-explain'>This tests whether the spread of bookings across Morning, Afternoon, Evening, and Late night is just random chance   or whether people genuinely prefer to buy tickets at a specific time of day. If the result is significant, you can trust the booking-time chart in the Revenue section and plan your campaigns around that window.</p>");
        String sigPlain = stats.chiSquaredP < 0.05
                ? "Yes   the pattern is statistically significant. Your audience has a clear preferred booking window."
                : "No clear preference detected   bookings are spread fairly evenly across the day.";
        statRow(html,"Is the booking time pattern meaningful?", sigPlain, stats.chiSquaredP < 0.05);
        html.append("<p class='algo-insight'>").append(stats.chiSquaredP < 0.05
                        ? "You can rely on the booking time chart. Send campaigns during your peak window for the best open and conversion rates."
                        : "With no strong time preference, the timing of your email campaigns is less critical. Focus on the content and the subject line instead.")
                .append("</p>");
        html.append("</div>");
        html.append("<div class='algo-card'><div class='algo-card-title'>How many people come back?</div>");
        html.append("<p class='algo-explain'>Retention shows what percentage of your audience has attended more than one show. The chart splits your entire database into people who returned and people who only came once. High retention means you are building a loyal community. Low retention means you are good at attracting new people but may not be converting them into regulars.</p>");
        statRow(html,"People who came back for another show",String.format(Locale.ROOT, "%.1f%%",stats.cohortRetentionRate),true);
        statRow(html,"Tickets that resulted in a physical check-in",String.format(Locale.ROOT, "%.1f%%",attendanceRate),false);
        html.append("<div class='chart-wrap' style='height:150px;margin-top:14px'><canvas id='cohortChart'></canvas></div>");
        html.append("<p class='algo-insight'>The attendance rate tells you how many tickets sold actually resulted in someone walking through the door. A significant gap between tickets sold and check-ins may indicate a no-show problem worth addressing   perhaps through reminder messages the day before.</p>");
        html.append("</div>");

        // Z-score table
        html.append("<div class='algo-card wide'><div class='algo-card-title'>Which shows performed unusually well or poorly?</div>");
        html.append("<p class='algo-explain'>This compares every show's ticket count against the average across your entire catalogue. Shows flagged as outliers sold significantly more or fewer tickets than what is normal for The One Room. This is not a judgement   a low-ticket outlier might have been a deliberate intimate show, and a high-ticket outlier might be your break-out hit. Either way, these are shows worth examining more closely.</p>");
        if(S>1){
            html.append("<table><thead><tr><th>#</th><th>Show</th><th>Tickets sold</th><th>Distance from average</th><th>Assessment</th></tr></thead><tbody>");
            for(int i=0;i<S;i++){
                double z=stats.showZScores[i];
                boolean out=Math.abs(z)>2;
                String distDesc = z > 0 ? String.format(Locale.ROOT, "%.1f× above average",Math.abs(z)) : String.format(Locale.ROOT, "%.1f× below average",Math.abs(z));
                html.append("<tr><td>").append(i+1).append("</td>")
                        .append("<td>").append(escapeHtml(data.showNames.get(i))).append("</td>")
                        .append("<td>").append(data.showTicketCounts.get(i)).append("</td>")
                        .append("<td class='").append(out?"rose":"").append("'>").append(distDesc).append("</td>")
                        .append("<td>").append(out?"<span class='tag-sig'>Standout show</span>":"<span class='tag-ns'>Typical</span>").append("</td></tr>");
            }
            html.append("</tbody></table>");
            html.append("<p class='algo-insight'>Standout shows   in either direction   are the ones to study. Understand what made your biggest shows sell out, and understand what held back your quieter ones. Both answers will help you programme better.</p>");
        } else {
            html.append("<p class='muted'>Add at least 2 shows to run this analysis.</p>");
        }
        html.append("</div>");

        // moving average chart
        html.append("<div class='algo-card wide'><div class='algo-card-title'>The true revenue trend over time</div>");
        html.append("<p class='algo-explain'>Year-to-year revenue can be noisy   one exceptional show or a quiet season can skew a single year's number. This chart smooths things out by averaging each year with the two years around it. The result is a cleaner picture of whether The One Room is growing, plateauing, or declining over the long run. If this line is rising, the business is heading in the right direction regardless of any single year's performance.</p>");
        if(stats.movingAvgRevenue.isEmpty()){
            html.append("<p class='muted'>Three or more years of data are needed to generate this chart.</p>");
        } else {
            html.append("<div class='chart-wrap'><canvas id='maChart'></canvas></div>");
            html.append("<p class='algo-insight'>Each point on this line represents the average revenue of that year and the two preceding years. A consistently rising line is the most reliable sign of sustainable growth.</p>");
        }
        html.append("</div>");

        html.append("</div>"); // algo-grid
        html.append("</section>");

        // ================================================================
        //  SECTION 4   REACH OPTIMISATION
        // ================================================================
        html.append("<section id='greedy'>");
        html.append("<h2 class='section-heading'>Reach Optimisation</h2>");
        html.append("<p class='section-desc'>If you could only send one email campaign, which show's list should you use? If you could only promote one genre, which one reaches the most people who have never heard from you before? This section answers those questions   it finds the most efficient path to reaching your entire audience.</p>");
        html.append("<hr class='section-divider'>");

        html.append("<div class='algo-grid'>");

        // set-cover result
        html.append("<div class='algo-card wide'><div class='algo-card-title'>The minimum number of shows to reach 80% of your audience</div>");
        html.append("<p class='algo-explain'>Rather than blasting your entire database every time, this table shows you the smartest order to reach people. It starts with the show whose contact list contains the most people not yet reached, then picks the next show that adds the most new contacts on top of that, and so on. The result is the shortest path to covering 80% of your total audience   with no unnecessary overlap.</p>");
        html.append("<p class='algo-explain'>Result: <strong class='gold'>").append(greedy.coverSet.size())
                .append(" show list").append(greedy.coverSet.size()==1?"":"s").append("</strong> covers <strong class='gold'>")
                .append(String.format(Locale.ROOT, "%.1f%%",greedy.coveragePct))
                .append("</strong> of all contacts (").append(greedy.totalCovered).append(" of ").append(greedy.totalContacts).append(" people).</p>");
        if(!greedy.coverSet.isEmpty()){
            html.append("<table><thead><tr><th>Step</th><th>Use this show list</th><th>New people reached</th><th>Running total</th><th>Coverage</th></tr></thead><tbody>");
            int cum=0;
            for(int i=0;i<greedy.coverSet.size();i++){
                cum+=greedy.coverMarginal.get(i);
                double pct=greedy.totalContacts==0?0:cum*100.0/greedy.totalContacts;
                html.append("<tr><td>").append(i+1).append("</td>")
                        .append("<td>").append(escapeHtml(greedy.coverSet.get(i))).append("</td>")
                        .append("<td><strong class='gold'>+").append(greedy.coverMarginal.get(i)).append("</strong></td>")
                        .append("<td>").append(cum).append("</td>")
                        .append("<td>").append(String.format(Locale.ROOT, "%.1f%%",pct)).append("</td></tr>");
            }
            html.append("</tbody></table>");
            html.append("<p class='algo-insight'>Each step adds fewer new people than the one before it   this is normal, because audiences overlap. The table stops once 80% is reached. To get the remaining 20%, you would need to send to increasingly smaller and more niche lists, which may not be worth the effort for a general campaign.</p>");
        }
        html.append("</div>");

        // category reach order
        html.append("<div class='algo-card'><div class='algo-card-title'>Best order to promote by genre</div>");
        html.append("<p class='algo-explain'>If you are running genre-specific campaigns one at a time, this is the order that maximises how many new, previously-unreached people you contact at each step. The first genre listed has the largest unique audience. Each genre after that is ranked by how many additional people it brings in beyond those already covered.</p>");
        html.append("<table><thead><tr><th>Priority</th><th>Genre</th><th>Additional people reached</th></tr></thead><tbody>");
        for(int i=0;i<greedy.catOrder.size();i++){
            html.append("<tr><td>").append(i+1).append("</td>")
                    .append("<td><strong class='gold'>").append(greedy.catOrder.get(i)).append("</strong></td>")
                    .append("<td>+").append(greedy.catMarginal.get(i)).append(" people</td></tr>");
        }
        html.append("</tbody></table>");
        html.append("<p class='algo-insight'>This order changes as your database grows. The genre at the top is simply the one with the most unique contacts right now   not necessarily the most popular genre overall.</p>");
        html.append("</div>");

        html.append("<div class='algo-card'><div class='algo-card-title'>How quickly each show list adds new contacts</div>");
        html.append("<p class='algo-explain'>This chart shows the number of new, previously-unreached people each show brings into the coverage. The bars get shorter as you progress because each subsequent show list has more overlap with the people already reached. A bar that drops sharply to near zero means further sends are largely duplicating coverage you already have.</p>");
        html.append("<div class='chart-wrap'><canvas id='greedyChart'></canvas></div></div>");

        html.append("</div>"); // algo-grid
        html.append("</section>");

        // ================================================================
        //  SECTION 5   SCHEDULE PLANNING
        // ================================================================
        html.append("<section id='dp'>");
        html.append("<h2 class='section-heading'>Schedule Planning</h2>");
        html.append("<p class='section-desc'>If you had to cut your programme in half   say, you could only run half as many shows next season   which shows should you keep to reach the most people? And which pairs of shows are so similar in audience that running both may not be worth it? This section helps answer those questions.</p>");
        html.append("<hr class='section-divider'>");

        html.append("<div class='algo-grid'>");

        // knapsack
        int halfBudget = Math.max(1, S/2);
        html.append("<div class='algo-card wide'><div class='algo-card-title'>If you could only run half your shows, which ones maximise your audience?</div>");
        html.append("<p class='algo-explain'>Working with a budget of ").append(halfBudget).append(" show slots (half of your current ").append(S)
                .append(" shows), this selects the combination that reaches the highest number of unique people. It is not simply picking the shows with the most tickets sold   it picks the shows whose audiences overlap the least with each other, so together they cover as much of the total audience as possible.</p>");
        html.append("<div class='kpi-row' style='margin-bottom:16px'>");
        kpi(html, String.valueOf(dp.optimalShows.size()), "Shows to keep");
        kpi(html, String.valueOf(dp.expectedAudience),    "Unique people reached");
        kpi(html, String.valueOf(S - dp.capacityUsed),    "Shows that can be rested");
        html.append("</div>");
        if(!dp.optimalShows.isEmpty()){
            html.append("<p style='font-size:13px;color:rgba(255,255,255,0.6);margin:0 0 10px'>Recommended shows to prioritise:</p>");
            html.append("<div class='show-chips'>");
            for(String s : dp.optimalShows)
                html.append("<span class='chip'>").append(escapeHtml(s)).append("</span>");
            html.append("</div>");
            html.append("<p class='algo-insight'>This is a planning tool, not a directive. A show that does not appear here may still be valuable for artistic, community, or brand reasons. Use this as a starting point for the conversation about which shows deliver the broadest audience reach for your investment.</p>");
        }
        html.append("</div>");

        // LCS
        html.append("<div class='algo-card wide'><div class='algo-card-title'>Which show pairs share the deepest audience overlap?</div>");
        html.append("<p class='algo-explain'>This goes deeper than the overlap score in the Audience Connections section. It compares the actual sequence of contacts who attended each show   not just whether they attended, but the pattern of who turned up together. A high number here means two shows share a substantial core audience in common. If two shows rank very high on this list, running both in the same season may reach fewer new people than you expect, since a large portion of their audiences are the same individuals.</p>");
        if(dp.lcsSimilarity.isEmpty()){
            html.append("<p class='muted'>Add at least 2 shows to see this comparison.</p>");
        } else {
            html.append("<table><thead><tr><th>Rank</th><th>Show A</th><th>Show B</th><th>Shared audience depth</th></tr></thead><tbody>");
            for(int i=0;i<dp.lcsSimilarity.size();i++){
                String[] row=dp.lcsSimilarity.get(i);
                html.append("<tr><td>").append(i+1).append("</td>")
                        .append("<td>").append(escapeHtml(row[0])).append("</td>")
                        .append("<td>").append(escapeHtml(row[1])).append("</td>")
                        .append("<td><strong class='gold'>").append(row[2]).append(" contacts in common</strong></td></tr>");
            }
            html.append("</tbody></table>");
            html.append("<p class='algo-insight'>Show pairs at the top of this list are worth spacing out across the calendar rather than running close together. Their audiences are so similar that running them back-to-back means you are largely marketing to the same people twice. Spreading them out gives each show time to attract someone new.</p>");
        }
        html.append("</div>");

        html.append("</div>"); // algo-grid
        html.append("</section>");


        // ================================================================
        //  SECTION 6   DEMOGRAPHICS
        // ================================================================
        html.append("<section id='demographics'>");
        html.append("<h2 class='section-heading'>Audience Demographics</h2>");
        html.append("<p class='section-desc'>An estimate of the gender split and cultural background of your audience, inferred from first names and surnames in your ticket data. These are broad indicators to help with programming and marketing decisions   not a definitive census. Age cannot be determined from the data currently collected by Webtickets.</p>");
        html.append("<div class='notice-box'>These estimates are based on name patterns only. No personal data is stored beyond the aggregate counts shown here.</div>");
        html.append("<hr class='section-divider'>");

        int demoTotal = demo.genderMale + demo.genderFemale + demo.genderUnknown;
        html.append("<div class='algo-grid'>");

        // GENDER
        html.append("<div class='algo-card'><div class='algo-card-title'>Estimated gender split</div>");
        html.append("<p class='algo-explain'>Classified by first name using a South African name dictionary spanning English, Xhosa, Zulu, Sotho, and Afrikaans naming traditions. Names used equally by both genders are listed as Unknown.</p>");
        if (demoTotal > 0) {
            double malePct   = demo.genderMale   * 100.0 / demoTotal;
            double femalePct = demo.genderFemale * 100.0 / demoTotal;
            double unknPct   = demo.genderUnknown* 100.0 / demoTotal;
            statRow(html, "Male",    demo.genderMale    + " contacts (" + String.format(Locale.ROOT, "%.0f%%", malePct)   + ")", true);
            statRow(html, "Female",  demo.genderFemale  + " contacts (" + String.format(Locale.ROOT, "%.0f%%", femalePct) + ")", true);
            statRow(html, "Unknown", demo.genderUnknown + " contacts (" + String.format(Locale.ROOT, "%.0f%%", unknPct)   + ")", false);
            html.append("<div class='chart-wrap' style='height:180px;margin-top:16px'><canvas id='genderChart'></canvas></div>");
            String gLeader = demo.genderFemale > demo.genderMale ? "female" : demo.genderMale > demo.genderFemale ? "male" : "";
            if (!gLeader.isEmpty()) {
                double gap = Math.abs(malePct - femalePct);
                String gapDesc = gap > 20 ? "significantly more" : gap > 8 ? "somewhat more" : "slightly more";
                html.append("<p class='algo-insight'>Your audience skews <strong>").append(gLeader).append("</strong>   there are ")
                        .append(gapDesc).append(" ").append(gLeader).append(" attendees. ");
                if (gLeader.equals("female")) html.append("This is typical for Afrosoul and Jazz events. Ensure marketing imagery speaks to this majority while remaining broadly welcoming.");
                else html.append("Consider whether your marketing channels are reaching female audiences as effectively   broadening appeal often grows the total audience.");
                html.append("</p>");
            } else {
                html.append("<p class='algo-insight'>Your audience is roughly balanced between male and female attendees   a sign of broad appeal.</p>");
            }
        } else {
            html.append("<p class='muted'>No name data found. Ensure your CSV files include First name and Surname columns.</p>");
        }
        html.append("</div>");

        // CULTURAL BACKGROUND
        int bgTotal = demo.bgBlackAfrican + demo.bgAfrikaans + demo.bgEnglish + demo.bgAsian + demo.bgOther + demo.bgUnknown;
        html.append("<div class='algo-card'><div class='algo-card-title'>Estimated cultural background</div>");
        html.append("<p class='algo-explain'>Inferred from surname patterns across South Africa's major naming traditions. Contacts whose surnames did not match a known pattern are listed as Unknown   this is normal in a diverse audience.</p>");
        if (bgTotal > 0) {
            double baPct = demo.bgBlackAfrican * 100.0 / bgTotal;
            double afPct = demo.bgAfrikaans    * 100.0 / bgTotal;
            double enPct = demo.bgEnglish      * 100.0 / bgTotal;
            double asPct = demo.bgAsian        * 100.0 / bgTotal;
            double otPct = demo.bgOther        * 100.0 / bgTotal;
            double unPct = demo.bgUnknown      * 100.0 / bgTotal;
            statRow(html, "Black African", demo.bgBlackAfrican + " (" + String.format(Locale.ROOT, "%.0f%%",baPct) + ")", true);
            statRow(html, "Afrikaans",     demo.bgAfrikaans    + " (" + String.format(Locale.ROOT, "%.0f%%",afPct) + ")", false);
            statRow(html, "English",       demo.bgEnglish      + " (" + String.format(Locale.ROOT, "%.0f%%",enPct) + ")", false);
            statRow(html, "Asian",         demo.bgAsian        + " (" + String.format(Locale.ROOT, "%.0f%%",asPct) + ")", false);
            if (demo.bgOther > 0) statRow(html, "Other", demo.bgOther + " (" + String.format(Locale.ROOT, "%.0f%%",otPct) + ")", false);
            statRow(html, "Unknown",       demo.bgUnknown      + " (" + String.format(Locale.ROOT, "%.0f%%",unPct) + ")", false);
            html.append("<div class='chart-wrap' style='height:180px;margin-top:16px'><canvas id='bgChart'></canvas></div>");
            html.append("<p class='algo-insight'>");
            if (demo.bgBlackAfrican > bgTotal * 0.5)
                html.append("The majority of your audience comes from Black African naming backgrounds, aligning with the club's focus on Afrosoul, Jazz, and genres rooted in African musical traditions.");
            else if (demo.bgUnknown > bgTotal * 0.4)
                html.append("A large portion of surnames could not be matched   common in diverse or international audiences. The known portion still provides a useful directional guide.");
            else
                html.append("Your audience appears culturally diverse. This is a strength   it suggests The One Room's music crosses community boundaries effectively.");
            html.append("</p>");
        } else {
            html.append("<p class='muted'>No surname data found.</p>");
        }
        html.append("</div>");

        // WHY NO AGE
        html.append("<div class='algo-card wide'><div class='algo-card-title'>Why age data is not available</div>");
        html.append("<p class='algo-explain'>Age cannot be determined from current ticket data. Webtickets does not collect date of birth or ID number at checkout. To enable age-range analysis in future, The One Room would need to add a voluntary age-range question at the point of purchase. The four ranges most useful for programming decisions are: Under 25, 25 to 34, 35 to 49, and 50 and over.</p>");
        html.append("</div>");

        html.append("</div>"); // algo-grid
        html.append("</section>");

        html.append("<p style='margin-top:24px;font-size:13px;'><a href='/'>&#8592; Back to dashboard</a></p>");

        // ================================================================
        //  CHART.JS SCRIPTS
        // ================================================================
        html.append("<script>");
        html.append("const g='#f2c46b',pu='#9b6ec8',te='#5cc8b8',ro='#e06b8b',sk='#6bade0',li='#8be06b';");
        html.append("const base={responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},");
        html.append("tooltip:{backgroundColor:'rgba(6,6,10,0.97)',borderColor:'rgba(242,196,107,0.25)',borderWidth:1,");
        html.append("titleColor:'#f8e7c3',bodyColor:'rgba(255,255,255,0.65)',padding:10}},");
        html.append("scales:{x:{grid:{color:'rgba(255,255,255,0.04)'},ticks:{color:'rgba(255,255,255,0.4)',font:{size:10}}},");
        html.append("y:{grid:{color:'rgba(255,255,255,0.04)'},ticks:{color:'rgba(255,255,255,0.4)',font:{size:10}}}}};");
        html.append("const donutBase={responsive:true,maintainAspectRatio:false,cutout:'62%',");
        html.append("plugins:{legend:{display:true,position:'bottom',labels:{color:'rgba(255,255,255,0.55)',font:{size:11},boxWidth:10,padding:10}},");
        html.append("tooltip:{backgroundColor:'rgba(6,6,10,0.97)',borderColor:'rgba(242,196,107,0.25)',borderWidth:1,");
        html.append("titleColor:'#f8e7c3',bodyColor:'rgba(255,255,255,0.65)',padding:10}}};");

        // revenue charts
        html.append("new Chart(document.getElementById('yearChart'),{type:'line',data:{labels:")
                .append(toJSStringArray(years)).append(",datasets:[{data:").append(toJSDoubleArray(yearRevArr))
                .append(",borderColor:g,backgroundColor:'rgba(242,196,107,0.08)',borderWidth:2,pointBackgroundColor:g,pointRadius:4,fill:true,tension:0.4}]},options:{...base}});");
        html.append("new Chart(document.getElementById('catChart'),{type:'bar',data:{labels:['Afrosoul','Jazz','Comedy','Folk','HipHop','Reggae'],datasets:[{data:")
                .append(toJSDoubleArray(catRevenue)).append(",backgroundColor:[g,pu,te,ro,sk,li],borderRadius:5,borderSkipped:false}]},options:{...base}});");
        html.append("new Chart(document.getElementById('retentionChart'),{type:'doughnut',data:{labels:['New','Returning'],datasets:[{data:[")
                .append(newC).append(",").append(retC).append("],backgroundColor:[te,g],borderWidth:0,hoverOffset:5}]},options:{...donutBase}});");
        html.append("new Chart(document.getElementById('timeChart'),{type:'doughnut',data:{labels:['Morning','Afternoon','Evening','Late'],datasets:[{data:")
                .append(toJSIntArray(data.timeBuckets)).append(",backgroundColor:[sk,g,pu,ro],borderWidth:0,hoverOffset:5}]},options:{...donutBase}});");
        html.append("new Chart(document.getElementById('groupChart'),{type:'bar',data:{labels:['1','2','3','4','5','6+'],datasets:[{data:")
                .append(toJSIntArray(data.groupSizeBuckets)).append(".slice(1),backgroundColor:g,borderRadius:5,borderSkipped:false}]},options:{...base}});");
        html.append("new Chart(document.getElementById('seasonChart'),{type:'bar',data:{labels:['Summer','Autumn','Winter','Spring'],datasets:[{data:[")
                .append(seasonRevMap.getOrDefault("Summer",0.0)).append(",").append(seasonRevMap.getOrDefault("Autumn",0.0)).append(",")
                .append(seasonRevMap.getOrDefault("Winter",0.0)).append(",").append(seasonRevMap.getOrDefault("Spring",0.0))
                .append("],backgroundColor:[g,ro,sk,li],borderRadius:5,borderSkipped:false}]},options:{...base}});");

        // graph charts
        html.append("new Chart(document.getElementById('degreeChart'),{type:'bar',data:{labels:")
                .append(toJSStringArray(data.showNames)).append(",datasets:[{data:").append(toJSIntArray(graph.showDegree))
                .append(",backgroundColor:g,borderRadius:5,borderSkipped:false}]},options:{...base}});");
        html.append("new Chart(document.getElementById('bridgeChart'),{type:'bar',data:{labels:")
                .append(toJSStringArray(data.showNames)).append(",datasets:[{data:").append(toJSIntArray(graph.showBridgeness))
                .append(",backgroundColor:pu,borderRadius:5,borderSkipped:false}]},options:{...base}});");

        // cohort doughnut
        int retCount=(int)(stats.cohortRetentionRate*C/100.0), oneCount=C-retCount;
        html.append("new Chart(document.getElementById('cohortChart'),{type:'doughnut',data:{labels:['Returned','One-time'],datasets:[{data:[")
                .append(retCount).append(",").append(oneCount).append("],backgroundColor:[g,pu],borderWidth:0,hoverOffset:4}]},options:{...donutBase}});");

        // MA chart
        html.append("const maChartEl=document.getElementById('maChart');if(maChartEl)new Chart(maChartEl,{type:'line',data:{labels:")
                .append(toJSStringArray(stats.movingAvgLabels)).append(",datasets:[{label:'MA(3)',data:")
                .append(toJSDoubleList(stats.movingAvgRevenue))
                .append(",borderColor:te,backgroundColor:'rgba(92,200,184,0.1)',borderWidth:2,pointBackgroundColor:te,pointRadius:4,fill:true,tension:0.4}]},options:{...base}});");

        // greedy marginal chart
        html.append("new Chart(document.getElementById('greedyChart'),{type:'bar',data:{labels:")
                .append(toJSStringArray(greedy.coverSet)).append(",datasets:[{label:'New contacts',data:")
                .append(toJSIntList(greedy.coverMarginal))
                .append(",backgroundColor:li,borderRadius:5,borderSkipped:false}]},options:{...base}});");

        // demographics doughnut charts
        html.append("const genderChartEl=document.getElementById('genderChart');if(genderChartEl)new Chart(genderChartEl,{type:'doughnut',data:{labels:['Male','Female','Unknown'],");
        html.append("datasets:[{data:[").append(demo.genderMale).append(",").append(demo.genderFemale).append(",").append(demo.genderUnknown);
        html.append("],backgroundColor:[sk,ro,'rgba(255,255,255,0.12)'],borderWidth:0,hoverOffset:5}]},options:{...donutBase}});" );
        html.append("const bgChartEl=document.getElementById('bgChart');if(bgChartEl)new Chart(bgChartEl,{type:'doughnut',");
        html.append("data:{labels:['Black African','Afrikaans','English','Asian','Other','Unknown'],");
        html.append("datasets:[{data:[").append(demo.bgBlackAfrican).append(",").append(demo.bgAfrikaans)
                .append(",").append(demo.bgEnglish).append(",").append(demo.bgAsian)
                .append(",").append(demo.bgOther).append(",").append(demo.bgUnknown);
        html.append("],backgroundColor:[g,pu,te,ro,li,'rgba(255,255,255,0.12)'],borderWidth:0,hoverOffset:5}]},options:{...donutBase}});");

        html.append("</script>");
        html.append("</div></body></html>");
        sendHtml(ex, html.toString());
    }

    // ================================================================
    //  INSIGHTS PAGE CSS
    // ================================================================

    private static String insightsCss() {
        return "<style>" +
                ":root{--g:#f2c46b;--pu:#9b6ec8;--te:#5cc8b8;--ro:#e06b8b;--sk:#6bade0;--li:#8be06b;}" +
                "*{box-sizing:border-box;}" +
                "body{margin:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;" +
                "background:radial-gradient(circle at top,#2b1b33 0,#0b0b10 40%,#050509 100%);color:#f0f0f0;min-height:100vh;}" +
                ".wrapper{max-width:1160px;margin:0 auto;padding:28px 18px 64px;}" +
                ".utility-nav{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:16px;padding:0 4px;}.utility-nav-group{display:flex;gap:8px;flex-wrap:wrap;}.utility-nav a{padding:7px 11px;border-radius:7px;border:1px solid rgba(255,255,255,.09);background:rgba(255,255,255,.035);font-size:12px;font-weight:650;}" +
                "a:focus-visible,button:focus-visible{outline:3px solid rgba(242,196,107,.5);outline-offset:2px;}" +
                // hero
                ".hero{border-radius:18px;padding:26px 28px;margin-bottom:22px;position:relative;overflow:hidden;" +
                "background:linear-gradient(135deg,rgba(255,204,128,0.06),rgba(255,255,255,0.02));" +
                "border:1px solid rgba(255,255,255,0.06);box-shadow:0 18px 45px rgba(0,0,0,0.75);}" +
                ".hero::before{content:'';position:absolute;right:-80px;top:-80px;width:220px;height:220px;" +
                "background:radial-gradient(circle,#f2c46b 0,rgba(242,196,107,0) 55%);opacity:0.7;pointer-events:none;}" +
                ".hero-pill{display:inline-block;padding:4px 10px;border-radius:999px;font-size:11px;" +
                "letter-spacing:0.16em;text-transform:uppercase;" +
                "background:rgba(0,0,0,0.55);border:1px solid rgba(255,255,255,0.12);color:#f8e7c3;margin-bottom:10px;}" +
                ".hero-title{font-size:26px;font-weight:600;margin:0 0 6px;letter-spacing:0.03em;text-transform:uppercase;position:relative;z-index:1;}" +
                ".hero-sub{font-size:14px;color:rgba(255,255,255,0.75);margin:0 0 6px;}" +
                ".hero-meta{font-size:12px;color:rgba(255,255,255,0.45);}" +
                // section nav
                ".section-nav{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:28px;}" +
                ".section-nav a{padding:6px 14px;border-radius:999px;font-size:12px;font-weight:600;" +
                "background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.09);" +
                "color:rgba(255,255,255,0.65);text-decoration:none;transition:all 0.18s;}" +
                ".section-nav a:hover{background:rgba(242,196,107,0.1);border-color:rgba(242,196,107,0.35);color:var(--g);}" +
                // sections
                "section{margin-bottom:44px;}" +
                ".section-heading{font-size:17px;font-weight:600;color:#f8e7c3;margin:0 0 4px;}" +
                ".section-desc{font-size:13px;color:rgba(255,255,255,0.55);margin:0 0 16px;line-height:1.6;}" +
                ".section-divider{border:none;border-top:1px solid rgba(255,255,255,0.06);margin:0 0 18px;}" +
                // kpi row
                ".kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin-bottom:16px;}" +
                ".kpi{border-radius:12px;padding:13px 16px;background:rgba(242,196,107,0.06);" +
                "border:1px solid rgba(242,196,107,0.15);}" +
                ".kpi-val{font-size:22px;font-weight:600;color:var(--g);letter-spacing:-0.01em;}" +
                ".kpi-label{font-size:11px;color:rgba(255,255,255,0.5);margin-top:3px;}" +
                // charts grid
                ".charts-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:4px;}" +
                ".chart-card{border-radius:14px;background:rgba(8,8,15,0.9);border:1px solid rgba(255,255,255,0.06);" +
                "box-shadow:0 8px 28px rgba(0,0,0,0.6);padding:18px 20px;}" +
                ".chart-card-title{font-size:14px;font-weight:600;color:#f8e7c3;margin:0 0 4px;}" +
                ".chart-explanation{font-size:13px;color:rgba(255,255,255,0.65);margin:0 0 12px;line-height:1.6;}" +
                ".chart-insight{font-size:12px;color:rgba(255,255,255,0.45);margin-top:10px;line-height:1.6;border-top:1px solid rgba(255,255,255,0.05);padding-top:10px;}" +
                ".chart-wrap{position:relative;height:200px;}" +
                // algo grid
                ".algo-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;}" +
                ".algo-card{border-radius:14px;background:rgba(8,8,15,0.9);border:1px solid rgba(255,255,255,0.06);overflow-x:auto;" +
                "box-shadow:0 8px 28px rgba(0,0,0,0.6);padding:20px 22px;}" +
                ".algo-card.wide{grid-column:span 2;}" +
                ".algo-card-title{font-size:14px;font-weight:600;color:#f8e7c3;margin:0 0 6px;}" +
                ".algo-explain{font-size:13px;color:rgba(255,255,255,0.65);line-height:1.65;margin:0 0 14px;}" +
                ".algo-insight{font-size:12px;color:rgba(255,255,255,0.45);line-height:1.6;margin:10px 0 0;" +
                "border-top:1px solid rgba(255,255,255,0.05);padding-top:10px;}" +
                // stat rows
                ".stat-row{display:flex;justify-content:space-between;align-items:center;" +
                "padding:8px 0;border-bottom:1px solid rgba(255,255,255,0.05);}" +
                ".stat-row:last-of-type{border-bottom:none;}" +
                ".stat-label{font-size:13px;color:rgba(255,255,255,0.6);}" +
                ".stat-val{font-size:14px;font-weight:600;color:#f0f0f0;}" +
                ".stat-val.gold{color:var(--g);}" +
                // table
                "table{width:100%;min-width:620px;border-collapse:collapse;font-size:14px;}" +
                "th,td{padding:8px 8px;border-bottom:1px solid rgba(255,255,255,0.05);text-align:left;}" +
                "th{font-size:11px;text-transform:uppercase;letter-spacing:0.08em;color:rgba(248,231,195,0.6);background:rgba(255,255,255,0.02);}" +
                "tbody tr:hover{background:rgba(255,255,255,0.02);}" +
                // utility
                ".gold{color:var(--g);}" +
                ".rose{color:#e06b8b;}" +
                ".tag-sig{display:inline-block;padding:2px 7px;border-radius:4px;font-size:11px;font-weight:600;" +
                "background:rgba(224,107,139,0.15);border:1px solid rgba(224,107,139,0.35);color:#e06b8b;}" +
                ".tag-ns{display:inline-block;padding:2px 7px;border-radius:4px;font-size:11px;" +
                "background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.1);color:rgba(255,255,255,0.45);}" +
                ".minibar{background:rgba(255,255,255,0.07);border-radius:3px;height:5px;width:70px;overflow:hidden;}" +
                ".minibar-fill{height:5px;background:var(--g);border-radius:3px;}" +
                ".show-chips{display:flex;flex-wrap:wrap;gap:8px;margin-top:10px;}" +
                ".chip{padding:5px 14px;border-radius:8px;font-size:13px;background:rgba(242,196,107,0.1);" +
                "border:1px solid rgba(242,196,107,0.25);color:var(--g);}" +
                "a{color:var(--g);text-decoration:none;}a:hover{color:#ffe3a5;text-decoration:underline;}" +
                ".muted{color:rgba(255,255,255,0.35);font-size:13px;}" +
                ".notice-box{background:rgba(242,196,107,0.06);border:1px solid rgba(242,196,107,0.2);border-radius:10px;padding:12px 16px;font-size:13px;color:rgba(255,255,255,0.6);line-height:1.6;margin:0 0 18px;}" +
                "@media(max-width:780px){.wrapper{padding:18px 12px 48px}.charts-grid,.algo-grid{grid-template-columns:1fr;}.algo-card.wide{grid-column:span 1;}.hero{padding:22px 18px}.section-nav{flex-wrap:nowrap;overflow-x:auto;padding-bottom:6px}.section-nav a{white-space:nowrap}.stat-row{align-items:flex-start;gap:10px}.stat-val{text-align:right}}" +
                "</style>";
    }

    // ── tiny HTML helpers ───────────────────────────────────────────
    private static void kpi(StringBuilder html, String val, String label) {
        html.append("<div class='kpi'><div class='kpi-val'>").append(val)
                .append("</div><div class='kpi-label'>").append(label).append("</div></div>");
    }
    private static void statRow(StringBuilder html, String label, String val, boolean gold) {
        html.append("<div class='stat-row'><span class='stat-label'>").append(label).append("</span>")
                .append("<span class='stat-val").append(gold?" gold":"").append("'>").append(val).append("</span></div>");
    }

    // ================================================================
    //  REACT FRONTEND + JSON API
    // ================================================================

    private static void handleReactApp(HttpExchange ex) throws IOException {
        String method = ex.getRequestMethod();
        if (!"GET".equalsIgnoreCase(method) && !"HEAD".equalsIgnoreCase(method)) {
            sendJson(ex, 405, "{\"error\":\"Method not allowed\"}");
            return;
        }

        Path dist = FRONTEND_DIST.toPath().toAbsolutePath().normalize();
        String requestPath = ex.getRequestURI().getPath();
        String relative = requestPath.startsWith("/") ? requestPath.substring(1) : requestPath;
        Path requested = dist.resolve(relative).normalize();
        Path asset = requested.startsWith(dist) && Files.isRegularFile(requested)
                ? requested
                : dist.resolve("index.html");

        if (!Files.isRegularFile(asset)) {
            sendHtml(ex, 503, "<!doctype html><html><head><meta charset='utf-8'><title>Frontend not built</title></head>"
                    + "<body><h1>React frontend not built</h1><p>Run <code>cd frontend &amp;&amp; npm install &amp;&amp; npm run build</code>, then restart the Java server.</p></body></html>");
            return;
        }

        byte[] bytes = Files.readAllBytes(asset);
        Headers headers = ex.getResponseHeaders();
        headers.set("Content-Type", contentTypeFor(asset.getFileName().toString()));
        headers.set("X-Content-Type-Options", "nosniff");
        headers.set("Cache-Control", requestPath.startsWith("/assets/")
                ? "public, max-age=31536000, immutable"
                : "no-cache");
        if ("HEAD".equalsIgnoreCase(method)) {
            ex.sendResponseHeaders(200, -1);
            ex.close();
            return;
        }
        ex.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    private static String contentTypeFor(String name) {
        String lower = name.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".html")) return "text/html; charset=UTF-8";
        if (lower.endsWith(".js")) return "text/javascript; charset=UTF-8";
        if (lower.endsWith(".css")) return "text/css; charset=UTF-8";
        if (lower.endsWith(".json") || lower.endsWith(".map")) return "application/json; charset=UTF-8";
        if (lower.endsWith(".svg")) return "image/svg+xml";
        if (lower.endsWith(".png")) return "image/png";
        if (lower.endsWith(".jpg") || lower.endsWith(".jpeg")) return "image/jpeg";
        if (lower.endsWith(".ico")) return "image/x-icon";
        if (lower.endsWith(".woff2")) return "font/woff2";
        return "application/octet-stream";
    }

    private static void handleApiHealth(HttpExchange ex) throws IOException {
        if (!requireMethod(ex, "GET")) return;
        sendJson(ex, 200, "{\"status\":\"ok\",\"frontendBuilt\":" + Files.isRegularFile(FRONTEND_DIST.toPath().resolve("index.html")) + "}");
    }

    private static void handleApiDashboard(HttpExchange ex) throws IOException {
        if (!requireMethod(ex, "GET")) return;
        File folder = new File(".");
        ProcessedData data = processFolder(folder);
        List<String> ovShows = new ArrayList<>(), ovCats = new ArrayList<>();
        loadCategoryOverrides(folder, ovShows, ovCats);
        int returning = 0;
        for (List<String> shows : data.contactShows) if (shows != null && shows.size() > 1) returning++;

        StringBuilder json = new StringBuilder("{");
        json.append("\"summary\":{")
                .append("\"totalShows\":").append(data.showNames.size()).append(',')
                .append("\"totalContacts\":").append(data.contactEmails.size()).append(',')
                .append("\"totalTickets\":").append(data.totalTickets).append(',')
                .append("\"returningAudience\":").append(returning).append("},");
        json.append("\"shows\":[");
        for (int i = 0; i < data.showNames.size(); i++) {
            if (i > 0) json.append(',');
            String show = data.showNames.get(i);
            json.append('{')
                    .append("\"id\":").append(i).append(',')
                    .append("\"name\":").append(jsStr(show)).append(',')
                    .append("\"category\":").append(jsStr(categoryForShow(show, ovShows, ovCats))).append(',')
                    .append("\"emailCount\":").append(data.showEmailsLists.get(i).size()).append(',')
                    .append("\"phoneCount\":").append(uniqueNonEmptyPhones(data.showPhoneLists.get(i)).size()).append(',')
                    .append("\"ticketCount\":").append(data.showTicketCounts.get(i))
                    .append('}');
        }
        json.append("],\"categories\":[");
        for (int i = 0; i < CATEGORIES.length; i++) {
            if (i > 0) json.append(',');
            String category = CATEGORIES[i];
            json.append('{')
                    .append("\"name\":").append(jsStr(category)).append(',')
                    .append("\"emailCount\":").append(getCategoryEmails(data, category).size()).append(',')
                    .append("\"phoneCount\":").append(uniqueNonEmptyPhones(getCategoryPhones(data, category)).size())
                    .append('}');
        }
        json.append("]}");
        sendJson(ex, 200, json.toString());
    }

    private static void handleApiShow(HttpExchange ex) throws IOException {
        if (!requireMethod(ex, "GET")) return;
        ProcessedData data = processFolder(new File("."));
        int index = parseIntQueryParam(ex.getRequestURI().getQuery(), "i", -1);
        if (index < 0 || index >= data.showNames.size()) {
            sendJson(ex, 404, "{\"error\":\"Show not found\"}");
            return;
        }
        String show = data.showNames.get(index);
        List<String> emails = data.showEmailsLists.get(index);
        List<String> phones = uniqueNonEmptyPhones(data.showPhoneLists.get(index));
        StringBuilder json = new StringBuilder("{")
                .append("\"id\":").append(index).append(',')
                .append("\"show\":").append(jsStr(show)).append(',')
                .append("\"emails\":");
        appendStringArray(json, emails);
        json.append(",\"phones\":[");
        for (int i = 0; i < phones.size(); i++) {
            if (i > 0) json.append(',');
            json.append("{\"name\":").append(jsStr(show + " " + String.format(Locale.ROOT, "%02d", i + 1)))
                    .append(",\"number\":").append(jsStr(phones.get(i))).append('}');
        }
        json.append("]}");
        sendJson(ex, 200, json.toString());
    }

    private static void handleApiCategory(HttpExchange ex) throws IOException {
        if (!requireMethod(ex, "GET")) return;
        File folder = new File(".");
        ProcessedData data = processFolder(folder);
        String category = normalizeCategory(getQueryParam(ex.getRequestURI().getQuery(), "cat"));
        if (category == null) {
            sendJson(ex, 404, "{\"error\":\"Category not found\"}");
            return;
        }
        List<String> ovShows = new ArrayList<>(), ovCats = new ArrayList<>();
        loadCategoryOverrides(folder, ovShows, ovCats);
        StringBuilder json = new StringBuilder("{\"category\":").append(jsStr(category)).append(",\"contacts\":[");
        boolean first = true;
        for (int i = 0; i < data.contactEmails.size(); i++) {
            List<String> attended = data.contactShows.get(i);
            List<String> matchingShows = new ArrayList<>();
            if (attended != null) {
                for (String show : attended) if (categoryForShow(show, ovShows, ovCats).equalsIgnoreCase(category)) matchingShows.add(show);
            }
            if (matchingShows.isEmpty()) continue;
            if (!first) json.append(',');
            first = false;
            json.append("{\"email\":").append(jsStr(data.contactEmails.get(i))).append(",\"shows\":");
            appendStringArray(json, matchingShows);
            json.append('}');
        }
        json.append("],\"phones\":[");
        List<String> phones = uniqueNonEmptyPhones(getCategoryPhones(data, category));
        for (int i = 0; i < phones.size(); i++) {
            if (i > 0) json.append(',');
            json.append("{\"name\":").append(jsStr(category + " " + String.format(Locale.ROOT, "%02d", i + 1)))
                    .append(",\"number\":").append(jsStr(phones.get(i))).append('}');
        }
        json.append("]}");
        sendJson(ex, 200, json.toString());
    }

    private static void handleApiInsights(HttpExchange ex) throws IOException {
        if (!requireMethod(ex, "GET")) return;
        File folder = new File(".");
        ProcessedData data = processFolder(folder);
        List<String> ovShows = new ArrayList<>(), ovCats = new ArrayList<>();
        loadCategoryOverrides(folder, ovShows, ovCats);
        GraphAnalysis graph = runGraphAnalysis(data, ovShows, ovCats);
        StatResult stats = runStatistics(data, graph, folder);
        GreedyResult greedy = runGreedy(data, graph, ovShows, ovCats);
        DPResult dp = runDP(data, graph);
        DemographicResult demographics = runDemographics(data);
        FinancialAnalysis finance = analyseFinances(folder, ovShows, ovCats);

        int contacts = data.contactEmails.size();
        int returning = 0;
        int attendedShows = 0;
        for (List<String> shows : data.contactShows) {
            int count = shows == null ? 0 : shows.size();
            attendedShows += count;
            if (count > 1) returning++;
        }
        int newAudience = Math.max(0, contacts - returning);
        double attendanceRate = finance.totalRows == 0 ? 0 : finance.checkedIn * 100.0 / finance.totalRows;
        double averageShows = contacts == 0 ? 0 : attendedShows * 1.0 / contacts;
        int bridgeIndex = -1;
        for (int i = 0; i < graph.showBridgeness.length; i++) {
            if (bridgeIndex < 0 || graph.showBridgeness[i] > graph.showBridgeness[bridgeIndex]) bridgeIndex = i;
        }

        StringBuilder json = new StringBuilder("{");
        json.append("\"summary\":{")
                .append("\"shows\":").append(data.showNames.size()).append(',')
                .append("\"contacts\":").append(contacts).append(',')
                .append("\"tickets\":").append(data.totalTickets).append(',')
                .append("\"totalRevenue\":"); appendJsonNumber(json, finance.totalRevenue);
        json.append(",\"returningAudience\":").append(returning)
                .append(",\"newAudience\":").append(newAudience)
                .append(",\"returningRate\":"); appendJsonNumber(json, contacts == 0 ? 0 : returning * 100.0 / contacts);
        json.append(",\"attendanceRate\":"); appendJsonNumber(json, attendanceRate);
        json.append(",\"averageShowsPerPerson\":"); appendJsonNumber(json, averageShows);
        json.append("},");

        json.append("\"revenue\":{");
        json.append("\"years\":[");
        int item = 0;
        for (Map.Entry<String, Double> entry : finance.yearlyRevenue.entrySet()) {
            if (item++ > 0) json.append(',');
            appendLabelValue(json, entry.getKey(), entry.getValue());
        }
        json.append("],\"categories\":[");
        for (int i = 0; i < CATEGORIES.length; i++) {
            if (i > 0) json.append(',');
            appendLabelValue(json, CATEGORIES[i], finance.categoryRevenue[i]);
        }
        json.append("],\"months\":[");
        String[] monthNames = {"January","February","March","April","May","June","July","August","September","October","November","December"};
        for (int month = 1; month <= 12; month++) {
            if (month > 1) json.append(',');
            appendLabelValue(json, monthNames[month - 1], finance.monthlyRevenue.getOrDefault(month, 0.0));
        }
        json.append("],\"seasons\":[");
        item = 0;
        for (Map.Entry<String, Double> entry : finance.seasonalRevenue.entrySet()) {
            if (item++ > 0) json.append(',');
            appendLabelValue(json, entry.getKey(), entry.getValue());
        }
        json.append("]},");

        String[] bookingLabels = {"Morning", "Afternoon", "Evening", "Late / Other"};
        json.append("\"audience\":{")
                .append("\"retention\":[");
        appendLabelValue(json, "New", newAudience); json.append(','); appendLabelValue(json, "Returning", returning);
        json.append("],\"bookingTimes\":[");
        for (int i = 0; i < data.timeBuckets.length; i++) {
            if (i > 0) json.append(',');
            appendLabelValue(json, bookingLabels[i], data.timeBuckets[i]);
        }
        json.append("],\"groupSizes\":[");
        for (int i = 1; i < data.groupSizeBuckets.length; i++) {
            if (i > 1) json.append(',');
            appendLabelValue(json, i == 6 ? "6+" : String.valueOf(i), data.groupSizeBuckets[i]);
        }
        json.append("]},");

        json.append("\"network\":{")
                .append("\"clusters\":").append(graph.contactComponents.size()).append(',')
                .append("\"crossGenreFans\":").append(graph.crossGenreFans).append(',')
                .append("\"bridgeShow\":").append(bridgeIndex < 0 ? "null" : jsStr(data.showNames.get(bridgeIndex))).append(',')
                .append("\"shows\":[");
        for (int i = 0; i < data.showNames.size(); i++) {
            if (i > 0) json.append(',');
            json.append("{\"name\":").append(jsStr(data.showNames.get(i)))
                    .append(",\"uniqueAudience\":").append(graph.showDegree[i])
                    .append(",\"bridgeness\":").append(graph.showBridgeness[i]).append('}');
        }
        json.append("],\"overlaps\":[");
        for (int i = 0; i < stats.topJaccardPairs.size(); i++) {
            if (i > 0) json.append(',');
            String[] pair = stats.topJaccardPairs.get(i);
            int firstShow = indexOfString(data.showNames, pair[0]);
            int secondShow = indexOfString(data.showNames, pair[1]);
            int shared = 0;
            if (firstShow >= 0 && secondShow >= 0) {
                for (int contact : graph.showToContacts.get(firstShow)) if (graph.showToContacts.get(secondShow).contains(contact)) shared++;
            }
            json.append("{\"showA\":").append(jsStr(pair[0]))
                    .append(",\"showB\":").append(jsStr(pair[1]))
                    .append(",\"score\":"); appendJsonNumber(json, Double.parseDouble(pair[2]));
            json.append(",\"sharedAudience\":").append(shared).append('}');
        }
        json.append("]},");

        json.append("\"patterns\":{")
                .append("\"pearsonCorrelation\":"); appendJsonNumber(json, stats.pearsonR);
        json.append(",\"pearsonInterpretation\":").append(jsStr(stats.pearsonInterp))
                .append(",\"chiSquared\":"); appendJsonNumber(json, stats.chiSquared);
        json.append(",\"chiSquaredPValue\":"); appendJsonNumber(json, stats.chiSquaredP);
        json.append(",\"cohortRetentionRate\":"); appendJsonNumber(json, stats.cohortRetentionRate);
        json.append(",\"movingAverage\":[");
        for (int i = 0; i < stats.movingAvgRevenue.size(); i++) {
            if (i > 0) json.append(',');
            appendLabelValue(json, stats.movingAvgLabels.get(i), stats.movingAvgRevenue.get(i));
        }
        json.append("],\"outliers\":[");
        for (int i = 0; i < data.showNames.size(); i++) {
            if (i > 0) json.append(',');
            double z = stats.showZScores[i];
            json.append("{\"show\":").append(jsStr(data.showNames.get(i)))
                    .append(",\"tickets\":").append(data.showTicketCounts.get(i))
                    .append(",\"zScore\":"); appendJsonNumber(json, z);
            json.append(",\"isOutlier\":").append(Math.abs(z) > 2).append('}');
        }
        json.append("]},");

        json.append("\"reach\":{")
                .append("\"targetPercentage\":"); appendJsonNumber(json, GreedyResult.TARGET * 100);
        json.append(",\"coveragePercentage\":"); appendJsonNumber(json, greedy.coveragePct);
        json.append(",\"coveredContacts\":").append(greedy.totalCovered)
                .append(",\"totalContacts\":").append(greedy.totalContacts)
                .append(",\"showOrder\":[");
        int cumulative = 0;
        for (int i = 0; i < greedy.coverSet.size(); i++) {
            if (i > 0) json.append(',');
            cumulative += greedy.coverMarginal.get(i);
            json.append("{\"step\":").append(i + 1)
                    .append(",\"show\":").append(jsStr(greedy.coverSet.get(i)))
                    .append(",\"newContacts\":").append(greedy.coverMarginal.get(i))
                    .append(",\"cumulativeContacts\":").append(cumulative).append('}');
        }
        json.append("],\"categoryOrder\":[");
        for (int i = 0; i < greedy.catOrder.size(); i++) {
            if (i > 0) json.append(',');
            json.append("{\"step\":").append(i + 1)
                    .append(",\"category\":").append(jsStr(greedy.catOrder.get(i)))
                    .append(",\"newContacts\":").append(greedy.catMarginal.get(i)).append('}');
        }
        json.append("]},");

        json.append("\"schedule\":{")
                .append("\"slotBudget\":").append(Math.max(1, data.showNames.size() / 2)).append(',')
                .append("\"expectedAudience\":").append(dp.expectedAudience).append(',')
                .append("\"shows\":"); appendStringArray(json, dp.optimalShows);
        json.append(",\"similarPairs\":[");
        for (int i = 0; i < dp.lcsSimilarity.size(); i++) {
            if (i > 0) json.append(',');
            String[] pair = dp.lcsSimilarity.get(i);
            json.append("{\"showA\":").append(jsStr(pair[0]))
                    .append(",\"showB\":").append(jsStr(pair[1]))
                    .append(",\"sharedAudienceDepth\":").append(Integer.parseInt(pair[2])).append('}');
        }
        json.append("]},");

        json.append("\"demographics\":{")
                .append("\"gender\":[");
        appendLabelValue(json, "Male", demographics.genderMale); json.append(',');
        appendLabelValue(json, "Female", demographics.genderFemale); json.append(',');
        appendLabelValue(json, "Unknown", demographics.genderUnknown);
        json.append("],\"background\":[");
        appendLabelValue(json, "Black African", demographics.bgBlackAfrican); json.append(',');
        appendLabelValue(json, "Afrikaans", demographics.bgAfrikaans); json.append(',');
        appendLabelValue(json, "English", demographics.bgEnglish); json.append(',');
        appendLabelValue(json, "Asian", demographics.bgAsian); json.append(',');
        appendLabelValue(json, "Other", demographics.bgOther); json.append(',');
        appendLabelValue(json, "Unknown", demographics.bgUnknown);
        json.append("]}}");
        sendJson(ex, 200, json.toString());
    }

    private static FinancialAnalysis analyseFinances(File folder, List<String> ovShows, List<String> ovCats) {
        FinancialAnalysis result = new FinancialAnalysis();
        File[] csvs = folder.listFiles((directory, name) -> name.toLowerCase(Locale.ROOT).endsWith(".csv")
                && !name.equalsIgnoreCase("contacts_with_shows.csv")
                && !name.equalsIgnoreCase("all_contacts.csv"));
        if (csvs == null) return result;
        Arrays.sort(csvs, Comparator.comparing(File::getName, String.CASE_INSENSITIVE_ORDER));
        for (File csv : csvs) {
            String base = csv.getName().substring(0, csv.getName().length() - 4);
            int categoryIndex = categoryIndex(categoryForShow(base, ovShows, ovCats)) - 1;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(csv), StandardCharsets.UTF_8))) {
                String header = reader.readLine();
                if (header == null) continue;
                char delimiter = detectDelimiter(header);
                List<String> columns = parseCsvLine(header, delimiter);
                int priceColumn = findIndex(columns, "Price paid");
                int purchaseDateColumn = findIndex(columns, "Purchase Date");
                int eventDateColumn = findIndex(columns, "EventDate");
                int checkedInColumn = findIndex(columns, "Checked In");
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;
                    List<String> row = parseCsvLine(line, delimiter);
                    double price = priceColumn >= 0 && priceColumn < row.size() ? parsePrice(row.get(priceColumn)) : 0;
                    result.totalRevenue += price;
                    if (categoryIndex >= 0 && categoryIndex < result.categoryRevenue.length) result.categoryRevenue[categoryIndex] += price;
                    String purchaseDate = cv(row, purchaseDateColumn);
                    if (purchaseDate.matches("\\d{4}.*")) result.yearlyRevenue.merge(purchaseDate.substring(0, 4), price, Double::sum);
                    String eventDate = cv(row, eventDateColumn);
                    if (!eventDate.isEmpty()) {
                        try {
                            LocalDate date = LocalDate.parse(eventDate.split(" ")[0]);
                            int month = date.getMonthValue();
                            result.monthlyRevenue.merge(month, price, Double::sum);
                            String season = month == 12 || month <= 2 ? "Summer" : month <= 5 ? "Autumn" : month <= 8 ? "Winter" : "Spring";
                            result.seasonalRevenue.merge(season, price, Double::sum);
                        } catch (Exception ignored) {}
                    }
                    if ("Yes".equalsIgnoreCase(cv(row, checkedInColumn))) result.checkedIn++;
                    result.totalRows++;
                }
            } catch (IOException ignored) {}
        }
        return result;
    }

    private static void handleApiReport(HttpExchange ex) throws IOException {
        if (!requireMethod(ex, "GET")) return;
        File folder = new File(".");
        ProcessedData data = processFolder(folder);
        List<String> ovShows = new ArrayList<>(), ovCats = new ArrayList<>();
        loadCategoryOverrides(folder, ovShows, ovCats);
        GraphAnalysis graph = runGraphAnalysis(data, ovShows, ovCats);
        StringBuilder json = new StringBuilder("{")
                .append("\"summary\":{")
                .append("\"contacts\":").append(data.contactEmails.size()).append(',')
                .append("\"shows\":").append(data.showNames.size()).append(',')
                .append("\"tickets\":").append(data.totalTickets).append("},")
                .append("\"contacts\":[");
        for (int i = 0; i < data.contactEmails.size(); i++) {
            if (i > 0) json.append(',');
            json.append("{\"email\":").append(jsStr(data.contactEmails.get(i)))
                    .append(",\"phone\":").append(jsStr(data.contactPhones.get(i)))
                    .append(",\"shows\":");
            List<String> shows = data.contactShows.get(i);
            appendStringArray(json, shows == null ? Collections.emptyList() : shows);
            json.append(",\"firstName\":").append(jsStr(data.contactFirstNames.size() > i ? data.contactFirstNames.get(i) : ""))
                    .append(",\"surname\":").append(jsStr(data.contactSurnames.size() > i ? data.contactSurnames.get(i) : ""))
                    .append('}');
        }
        json.append("],\"shows\":[");
        for (int i = 0; i < data.showNames.size(); i++) {
            if (i > 0) json.append(',');
            String show = data.showNames.get(i);
            json.append("{\"name\":").append(jsStr(show))
                    .append(",\"category\":").append(jsStr(categoryForShow(show, ovShows, ovCats)))
                    .append(",\"tickets\":").append(data.showTicketCounts.get(i))
                    .append(",\"uniqueContacts\":").append(graph.showToContacts.get(i).size()).append('}');
        }
        json.append("],\"categories\":[");
        for (int i = 0; i < CATEGORIES.length; i++) {
            if (i > 0) json.append(',');
            String category = CATEGORIES[i];
            json.append("{\"name\":").append(jsStr(category))
                    .append(",\"uniqueEmails\":").append(getCategoryEmails(data, category).size())
                    .append(",\"uniquePhones\":").append(uniqueNonEmptyPhones(getCategoryPhones(data, category)).size()).append('}');
        }
        json.append("]}");
        sendJson(ex, 200, json.toString());
    }

    private static void handleApiSettings(HttpExchange ex) throws IOException {
        File folder = new File(".");
        if ("POST".equalsIgnoreCase(ex.getRequestMethod())) {
            String kind = getQueryParam(ex.getRequestURI().getQuery(), "kind");
            String body = readBody(ex);
            if ("exclusions".equals(kind)) {
                String text = getFormParam(body, "emailsText");
                List<String> emails = new ArrayList<>();
                if (text != null) {
                    try (BufferedReader reader = new BufferedReader(new StringReader(text))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            String email = line.trim().toLowerCase(Locale.ROOT);
                            if (!email.isEmpty() && !containsString(emails, email)) emails.add(email);
                        }
                    }
                }
                try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream(new File(folder, "excluded_emails.txt")), StandardCharsets.UTF_8))) {
                    for (String email : emails) { writer.write(email); writer.newLine(); }
                }
                invalidateDataCache();
            } else if ("categories".equals(kind)) {
                int total = 0;
                try { total = Integer.parseInt(Objects.toString(getFormParam(body, "totalShows"), "0")); }
                catch (NumberFormatException ignored) {}
                try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream(new File(folder, "categories.txt")), StandardCharsets.UTF_8))) {
                    for (int i = 0; i < total; i++) {
                        String show = Objects.toString(getFormParam(body, "show_" + i), "").trim();
                        String category = normalizeCategory(getFormParam(body, "cat_" + i));
                        if (show.isEmpty() || category == null) continue;
                        writer.write(escapeCsv(show));
                        writer.write(',');
                        writer.write(escapeCsv(category));
                        writer.newLine();
                    }
                }
                invalidateDataCache();
            } else {
                sendJson(ex, 400, "{\"error\":\"Unknown settings type\"}");
                return;
            }
        } else if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            ex.getResponseHeaders().set("Allow", "GET, POST");
            sendJson(ex, 405, "{\"error\":\"Method not allowed\"}");
            return;
        }
        sendJson(ex, 200, buildSettingsJson(folder));
    }

    private static String buildSettingsJson(File folder) {
        ProcessedData data = processFolder(folder);
        List<String> overrideShows = new ArrayList<>(), overrideCategories = new ArrayList<>();
        loadCategoryOverrides(folder, overrideShows, overrideCategories);
        List<String> exclusions = loadExcludedEmails(folder);
        StringBuilder json = new StringBuilder("{\"categories\":");
        appendStringArray(json, Arrays.asList(CATEGORIES));
        json.append(",\"excludedEmails\":");
        appendStringArray(json, exclusions);
        json.append(",\"shows\":[");
        for (int i = 0; i < data.showNames.size(); i++) {
            if (i > 0) json.append(',');
            String show = data.showNames.get(i);
            String guess = categoryGuess(show);
            String override = "";
            for (int j = 0; j < overrideShows.size(); j++) {
                if (overrideShows.get(j).equalsIgnoreCase(show)) {
                    String normalized = normalizeCategory(overrideCategories.get(j));
                    if (normalized != null) override = normalized;
                    break;
                }
            }
            json.append("{\"name\":").append(jsStr(show))
                    .append(",\"automaticCategory\":").append(jsStr(guess))
                    .append(",\"override\":").append(jsStr(override))
                    .append(",\"currentCategory\":").append(jsStr(override.isEmpty() ? guess : override)).append('}');
        }
        return json.append("]}").toString();
    }

    private static void handleApiUpload(HttpExchange ex) throws IOException {
        if (!requireMethod(ex, "POST")) return;
        UploadResult result = receiveCsvUpload(ex);
        if (result.error != null) {
            sendJson(ex, result.status, "{\"error\":" + jsStr(result.error) + "}");
            return;
        }
        sendJson(ex, result.status, "{\"message\":\"CSV uploaded successfully\",\"fileName\":" + jsStr(result.fileName) + "}");
    }

    private static void appendLabelValue(StringBuilder json, String label, double value) {
        json.append("{\"label\":").append(jsStr(label)).append(",\"value\":");
        appendJsonNumber(json, value);
        json.append('}');
    }

    private static void appendJsonNumber(StringBuilder json, double value) {
        if (Double.isFinite(value)) json.append(String.format(Locale.ROOT, "%.4f", value));
        else json.append("null");
    }

    private static boolean requireMethod(HttpExchange ex, String expected) throws IOException {
        if (expected.equalsIgnoreCase(ex.getRequestMethod())) return true;
        ex.getResponseHeaders().set("Allow", expected);
        sendJson(ex, 405, "{\"error\":\"Method not allowed\"}");
        return false;
    }

    private static void sendJson(HttpExchange ex, int status, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        Headers headers = ex.getResponseHeaders();
        headers.set("Content-Type", "application/json; charset=UTF-8");
        headers.set("Cache-Control", "no-store");
        headers.set("X-Content-Type-Options", "nosniff");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    private static void appendStringArray(StringBuilder json, Collection<String> values) {
        json.append('[');
        boolean first = true;
        for (String value : values) {
            if (!first) json.append(',');
            first = false;
            json.append(jsStr(value));
        }
        json.append(']');
    }

    // ================================================================
    //  LEGACY SERVER-RENDERED PAGES (kept for reference during migration)
    // ================================================================

    private static void handleRoot(HttpExchange ex) throws IOException {
        File folder = new File(".");
        ProcessedData data = processFolder(folder);
        List<String> ovS = new ArrayList<>(), ovC = new ArrayList<>();
        loadCategoryOverrides(folder, ovS, ovC);

        int totalShows = data.showNames.size();
        int totalContacts = data.contactEmails.size();
        int totalTickets = data.totalTickets;
        int returners = 0;
        for (List<String> sh : data.contactShows) if (sh != null && sh.size() > 1) returners++;
        String uploadStatus = getQueryParam(ex.getRequestURI().getQuery(),"upload");

        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html><html><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width,initial-scale=1'>");
        html.append("<title>The One Room</title>");
        html.append(rootCss());
        html.append("</head><body>");
        html.append("<a class='skip-link' href='#main-content'>Skip to dashboard content</a>");

        // TOP NAV
        html.append("<nav class='topbar' aria-label='Primary navigation'>");
        html.append("<div class='topbar-brand'>");
        html.append("<div class='topbar-logo'><img src='/logo' alt='The One Room logo'></div>");
        html.append("<span class='topbar-name'>The One Room</span>");
        html.append("</div>");
        html.append("<div class='topbar-nav'>");
        html.append("<a href='/' class='active'>Dashboard</a>");
        html.append("<a href='/dataInsights'>Insights</a>");
        html.append("<a href='/report'>Reports</a>");
        html.append("<a href='/exclusions'>Settings</a>");
        html.append("</div>");
        html.append("</nav>");

        html.append("<main class='page' id='main-content'>");
        if("success".equals(uploadStatus))html.append("<div class='toast toast-success' role='status'>CSV uploaded successfully. Dashboard data is up to date.</div>");

        // PAGE HEADER
        html.append("<div class='page-header'>");
        html.append("<div class='eyebrow'>Audience operations</div>");
        html.append("<h1>Audience Dashboard</h1>");
        html.append("<p>Search every show, compare genre reach, and prepare your next audience campaign.</p>");
        html.append("</div>");

        // STAT STRIP
        html.append("<div class='stat-strip'>");
        html.append("<div class='stat-strip-item'><div class='stat-strip-val'>").append(totalShows).append("</div><div class='stat-strip-label'>Shows loaded</div></div>");
        html.append("<div class='stat-strip-item'><div class='stat-strip-val'>").append(totalContacts).append("</div><div class='stat-strip-label'>Unique contacts</div></div>");
        html.append("<div class='stat-strip-item'><div class='stat-strip-val'>").append(totalTickets).append("</div><div class='stat-strip-label'>Total tickets</div></div>");
        html.append("<div class='stat-strip-item'><div class='stat-strip-val'>").append(returners).append("</div><div class='stat-strip-label'>Returning audience</div></div>");
        html.append("</div>");

        // ACTION BUTTONS
        html.append("<div class='action-buttons'>");
        html.append("<a href='/dataInsights' class='btn btn-primary'>Open insights</a>");
        html.append("<a href='/report' class='btn btn-outline'>Generate report</a>");
        html.append("</div>");

        // MAIN GRID
        html.append("<div class='main-grid'>");

        // LEFT: SHOWS TABLE
        html.append("<div>");
        html.append("<section class='card' aria-labelledby='shows-heading'>");
        html.append("<div class='card-header'><div><div class='eyebrow'>Database</div><h2 id='shows-heading'>Shows</h2></div><p>").append(totalShows).append(" CSV files loaded</p></div>");
        if (data.showNames.isEmpty()) {
            html.append("<div class='empty-state'><strong>No shows loaded yet</strong><p>Upload a Webtickets CSV to build your audience dashboard.</p></div>");
        } else {
            html.append("<div class='table-tools'>");
            html.append("<label class='field search-field'><span>Search shows</span><input id='showSearch' type='search' placeholder='Artist, event, or genre' autocomplete='off'></label>");
            html.append("<label class='field'><span>Category</span><select id='categoryFilter'><option value=''>All categories</option>");
            for(String category:CATEGORIES)html.append("<option value='").append(escapeHtmlAttr(category.toLowerCase(Locale.ROOT))).append("'>").append(escapeHtml(category)).append("</option>");
            html.append("</select></label><div class='result-count' id='showResultCount' role='status' aria-live='polite'></div></div>");
            html.append("<div class='table-scroll'><table id='showsTable'><caption class='sr-only'>Shows with category and audience contact totals</caption><thead><tr>");
            html.append("<th scope='col'>#</th><th scope='col' aria-sort='ascending'><button class='sort-button' type='button' data-sort='name'>Show <span aria-hidden='true'>↕</span></button></th>");
            html.append("<th scope='col'><button class='sort-button' type='button' data-sort='category'>Category <span aria-hidden='true'>↕</span></button></th>");
            html.append("<th scope='col'><button class='sort-button' type='button' data-sort='emails'>Emails <span aria-hidden='true'>↕</span></button></th>");
            html.append("<th scope='col'><button class='sort-button' type='button' data-sort='phones'>Numbers <span aria-hidden='true'>↕</span></button></th><th scope='col'>View</th></tr></thead><tbody id='showsTableBody'>");
            for (int i = 0; i < data.showNames.size(); i++) {
                String show = data.showNames.get(i);
                String cat  = categoryForShow(show, ovS, ovC);
                int ec = data.showEmailsLists.get(i).size();
                int pc = uniqueNonEmptyPhones(data.showPhoneLists.get(i)).size();
                html.append("<tr data-name='").append(escapeHtmlAttr(show.toLowerCase(Locale.ROOT))).append("' data-category='").append(escapeHtmlAttr(cat.toLowerCase(Locale.ROOT))).append("' data-emails='").append(ec).append("' data-phones='").append(pc).append("'>");
                html.append("<td class='row-number'>").append(i+1).append("</td>");
                html.append("<td style='font-weight:500'>").append(escapeHtml(show)).append("</td>");
                html.append("<td><span class='badge'>").append(escapeHtml(cat)).append("</span></td>");
                html.append("<td>").append(ec).append("</td>");
                html.append("<td>").append(pc).append("</td>");
                html.append("<td style='white-space:nowrap'>");
                html.append("<a href='/showEmails?i=").append(i).append("'>Emails</a>");
                if (pc > 0) html.append(" &middot; <a href='/showPhones?i=").append(i).append("'>Numbers</a>");
                html.append("</td></tr>");
            }
            html.append("<tr id='noShowResults' hidden><td colspan='6'><div class='empty-state compact'><strong>No matching shows</strong><p>Try a different search or category.</p></div></td></tr>");
            html.append("</tbody></table></div>");
            html.append("<div class='table-pagination'><button type='button' class='btn btn-outline btn-small' id='previousShows'>Previous</button><span id='showPageStatus' aria-live='polite'></span><button type='button' class='btn btn-outline btn-small' id='nextShows'>Next</button></div>");
        }
        html.append("</section>"); // card

        // CATEGORY TABLE
        html.append("<div class='card'>");
        html.append("<div class='card-header'><h2>By Category</h2><p>Reach per genre lane</p></div>");
        html.append("<div class='table-scroll'><table><caption class='sr-only'>Audience reach by category</caption><thead><tr><th scope='col'>Category</th><th scope='col'>Emails</th><th scope='col'>Numbers</th><th scope='col'>View</th></tr></thead><tbody>");
        addCategoryRow(html,"Afrosoul",data.catAfroEmails,data.catAfroPhones);
        addCategoryRow(html,"Jazz",data.catJazzEmails,data.catJazzPhones);
        addCategoryRow(html,"Comedy",data.catComedyEmails,data.catComedyPhones);
        addCategoryRow(html,"Folk",data.catPoetryEmails,data.catPoetryPhones);
        addCategoryRow(html,"HipHop",data.catHipHopEmails,data.catHipHopPhones);
        addCategoryRow(html,"Reggae",data.catReggaeEmails,data.catReggaePhones);
        html.append("</tbody></table></div>");
        html.append("</div>"); // card
        html.append("</div>"); // left col

        // RIGHT: SIDEBAR
        html.append("<aside aria-label='Dashboard tools'>");

        // All contacts
        html.append("<div class='sidebar-card'>");
        html.append("<h3>All Contacts</h3>");
        html.append("<div class='sidebar-stat'><span class='sidebar-stat-label'>Unique contacts</span><span class='sidebar-stat-val'>").append(totalContacts).append("</span></div>");
        html.append("<div class='sidebar-stat'><span class='sidebar-stat-label'>Returning</span><span class='sidebar-stat-val'>").append(returners).append("</span></div>");
        html.append("<div class='sidebar-stat'><span class='sidebar-stat-label'>Full export file</span><span class='sidebar-stat-val' style='font-size:12px'>contacts_with_shows.csv</span></div>");
        html.append("</div>");

        // Upload
        html.append("<div class='sidebar-card'>");
        html.append("<h3>Upload CSV</h3>");
        html.append("<form id='uploadForm' method='post' action='/upload' enctype='multipart/form-data'>");
        html.append("<label class='upload-drop' for='csvFile'><span class='upload-title'>Choose a Webtickets CSV</span><span id='selectedFile'>No file selected</span><input id='csvFile' type='file' name='csvFile' accept='.csv,text/csv' required></label>");
        html.append("<button id='uploadButton' type='submit' style='width:100%'>Upload and refresh</button>");
        html.append("<p class='form-status' id='uploadFormStatus' role='status' aria-live='polite'></p>");
        html.append("</form>");
        html.append("</div>");

        // Settings
        html.append("<div class='sidebar-card'>");
        html.append("<h3>Settings</h3>");
        html.append("<div style='display:flex;flex-direction:column;gap:8px'>");
        html.append("<a href='/categories' class='btn btn-outline' style='text-align:center;display:block'>Edit show categories</a>");
        html.append("<a href='/exclusions' class='btn btn-outline' style='text-align:center;display:block'>Manage excluded emails</a>");
        html.append("</div>");
        html.append("</div>");

        html.append("</aside>"); // sidebar
        html.append("</div>"); // main-grid

        html.append("</main>"); // page
        html.append("<div class='footer'>The One Room Music and Comedy Club</div>");
        html.append(rootDashboardScript());
        html.append("</body></html>");
        sendHtml(ex, html.toString());
    }


    // ================================================================
    //  REMAINING HANDLERS
    // ================================================================

    private static void handleShowEmails(HttpExchange ex) throws IOException {
        ProcessedData data=processFolder(new File("."));
        int idx=parseIntQueryParam(ex.getRequestURI().getQuery(),"i",-1);
        StringBuilder html=new StringBuilder();
        html.append("<!DOCTYPE html><html><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Show emails</title>").append(sharedCss()).append(sharedUiCss()).append("</head><body><div class='wrapper'>");
        if(idx<0||idx>=data.showNames.size()){html.append("<div class='card'><p class='muted'>Invalid show.</p><p><a href='/'>Back</a></p></div></div></body></html>");sendHtml(ex,html.toString());return;}
        String show=data.showNames.get(idx); List<String> emails=data.showEmailsLists.get(idx);
        html.append("<div class='hero'><div class='hero-pill'>Show emails</div><h1 class='hero-title'>").append(escapeHtml(show)).append("</h1><p class='hero-meta'>").append(emails.size()).append(" unique emails</p></div>");
        html.append("<div class='card'><table><thead><tr><th>#</th><th>Email</th></tr></thead><tbody>");
        for(int i=0;i<emails.size();i++) html.append("<tr><td>").append(i+1).append("</td><td>").append(escapeHtml(emails.get(i))).append("</td></tr>");
        html.append("</tbody></table><p style='margin-top:14px'><a href='/download?kind=showEmails&i=").append(idx).append("'>Download CSV</a> &nbsp;&middot;&nbsp; <a href='/'>&#8592; Back</a></p></div></div></body></html>");
        sendHtml(ex,html.toString());
    }

    private static void handleShowPhones(HttpExchange ex) throws IOException {
        ProcessedData data=processFolder(new File("."));
        int idx=parseIntQueryParam(ex.getRequestURI().getQuery(),"i",-1);
        StringBuilder html=new StringBuilder();
        html.append("<!DOCTYPE html><html><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Show numbers</title>").append(sharedCss()).append(sharedUiCss()).append("</head><body><div class='wrapper'>");
        if(idx<0||idx>=data.showNames.size()){html.append("<div class='card'><p class='muted'>Invalid show.</p><p><a href='/'>Back</a></p></div></div></body></html>");sendHtml(ex,html.toString());return;}
        String show=data.showNames.get(idx); List<String> phones=uniqueNonEmptyPhones(data.showPhoneLists.get(idx));
        html.append("<div class='hero'><div class='hero-pill'>Show numbers</div><h1 class='hero-title'>").append(escapeHtml(show)).append("</h1><p class='hero-meta'>").append(phones.size()).append(" unique numbers</p></div>");
        html.append("<div class='card'><table><thead><tr><th>#</th><th>Name</th><th>Number</th></tr></thead><tbody>");
        int ct=1; for(String p:phones){String nm=ct<10?"0"+ct:String.valueOf(ct);html.append("<tr><td>").append(ct).append("</td><td>").append(escapeHtml(show+" "+nm)).append("</td><td>").append(escapeHtml(p)).append("</td></tr>");ct++;}
        html.append("</tbody></table><p style='margin-top:14px'><a href='/download?kind=showPhones&i=").append(idx).append("'>Download CSV</a> &nbsp;&middot;&nbsp; <a href='/'>&#8592; Back</a></p></div></div></body></html>");
        sendHtml(ex,html.toString());
    }

    private static void handleCategoryContacts(HttpExchange ex) throws IOException {
        File folder=new File("."); ProcessedData data=processFolder(folder);
        String cat=normalizeCategory(getQueryParam(ex.getRequestURI().getQuery(),"cat"));
        if(cat==null){sendHtml(ex,"<html><body><p>Invalid category.</p><a href='/'>Back</a></body></html>");return;}
        List<String> ovS=new ArrayList<>(),ovC=new ArrayList<>(); loadCategoryOverrides(folder,ovS,ovC);
        List<String> emails=new ArrayList<>(),shown=new ArrayList<>();
        for(int i=0;i<data.contactEmails.size();i++){
            List<String> cs=data.contactShows.get(i); List<String> inCat=new ArrayList<>();
            for(String s:cs) if(categoryForShow(s,ovS,ovC).equalsIgnoreCase(cat)) inCat.add(s);
            if(!inCat.isEmpty()){StringBuilder sb=new StringBuilder();for(int j=0;j<inCat.size();j++){if(j>0)sb.append(" | ");sb.append(inCat.get(j));}emails.add(data.contactEmails.get(i)==null?"":data.contactEmails.get(i));shown.add(sb.toString());}
        }
        StringBuilder html=new StringBuilder();
        html.append("<!DOCTYPE html><html><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>").append(escapeHtml(cat)).append(" emails</title>").append(sharedCss()).append(sharedUiCss()).append("</head><body><div class='wrapper'>");
        html.append("<div class='hero'><div class='hero-pill'>Category emails</div><h1 class='hero-title'>").append(escapeHtml(cat)).append("</h1><p class='hero-meta'>").append(emails.size()).append(" contacts</p></div>");
        html.append("<div class='card'><table><thead><tr><th>#</th><th>Email</th><th>Shows</th></tr></thead><tbody>");
        for(int i=0;i<emails.size();i++) html.append("<tr><td>").append(i+1).append("</td><td>").append(escapeHtml(emails.get(i))).append("</td><td>").append(escapeHtml(shown.get(i))).append("</td></tr>");
        html.append("</tbody></table><p style='margin-top:14px'><a href='/download?kind=categoryContacts&cat=").append(cat).append("'>Download CSV</a> &nbsp;&middot;&nbsp; <a href='/'>&#8592; Back</a></p></div></div></body></html>");
        sendHtml(ex,html.toString());
    }

    private static void handleCategoryPhones(HttpExchange ex) throws IOException {
        ProcessedData data=processFolder(new File("."));
        String cat=normalizeCategory(getQueryParam(ex.getRequestURI().getQuery(),"cat"));
        if(cat==null){sendHtml(ex,"<html><body><p>Invalid category.</p><a href='/'>Back</a></body></html>");return;}
        List<String> phones=uniqueNonEmptyPhones(getCategoryPhones(data,cat));
        StringBuilder html=new StringBuilder();
        html.append("<!DOCTYPE html><html><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>").append(escapeHtml(cat)).append(" numbers</title>").append(sharedCss()).append(sharedUiCss()).append("</head><body><div class='wrapper'>");
        html.append("<div class='hero'><div class='hero-pill'>Category numbers</div><h1 class='hero-title'>").append(escapeHtml(cat)).append("</h1><p class='hero-meta'>").append(phones.size()).append(" unique numbers</p></div>");
        html.append("<div class='card'><table><thead><tr><th>#</th><th>Name</th><th>Number</th></tr></thead><tbody>");
        int ct=1; for(String p:phones){String nm=ct<10?"0"+ct:String.valueOf(ct);html.append("<tr><td>").append(ct).append("</td><td>").append(escapeHtml(cat+" "+nm)).append("</td><td>").append(escapeHtml(p)).append("</td></tr>");ct++;}
        html.append("</tbody></table><p style='margin-top:14px'><a href='/download?kind=categoryPhones&cat=").append(cat).append("'>Download CSV</a> &nbsp;&middot;&nbsp; <a href='/'>&#8592; Back</a></p></div></div></body></html>");
        sendHtml(ex,html.toString());
    }

    private static void handleLogo(HttpExchange ex) throws IOException {
        File folder=new File("."); File use=null; String mime="image/png";
        File png=new File(folder,"one_room_logo.png"),jpg=new File(folder,"one_room_logo.jpg");
        if(png.exists())use=png; else if(jpg.exists()){use=jpg;mime="image/jpeg";}
        else{File[]imgs=folder.listFiles((d,n)->n.toLowerCase().endsWith(".png")||n.toLowerCase().endsWith(".jpg")||n.toLowerCase().endsWith(".jpeg"));if(imgs!=null&&imgs.length>0){use=imgs[0];mime=use.getName().toLowerCase().endsWith(".png")?"image/png":"image/jpeg";}}
        if(use==null){ex.sendResponseHeaders(404,0);ex.getResponseBody().close();return;}
        byte[]bytes;try(InputStream in=new FileInputStream(use)){ByteArrayOutputStream b=new ByteArrayOutputStream();byte[]buf=new byte[4096];int r;while((r=in.read(buf))!=-1)b.write(buf,0,r);bytes=b.toByteArray();}
        ex.getResponseHeaders().set("Content-Type",mime);ex.sendResponseHeaders(200,bytes.length);try(OutputStream os=ex.getResponseBody()){os.write(bytes);}
    }

    private static void handleDownload(HttpExchange ex) throws IOException {
        File folder=new File("."); ProcessedData data=processFolder(folder);
        String query=ex.getRequestURI().getQuery(),kind=getQueryParam(query,"kind"); if(kind==null)kind="";
        String fn="export.csv"; StringBuilder csv=new StringBuilder();
        if(kind.equals("showEmails")){int idx=parseIntQueryParam(query,"i",-1);if(idx<0||idx>=data.showNames.size()){send404(ex);return;}fn=sanitize(data.showNames.get(idx)+"_emails.csv");csv.append("Email\n");for(String e:data.showEmailsLists.get(idx))csv.append(escapeCsv(e)).append("\n");}
        else if(kind.equals("showPhones")){int idx=parseIntQueryParam(query,"i",-1);if(idx<0||idx>=data.showNames.size()){send404(ex);return;}String show=data.showNames.get(idx);List<String>ph=uniqueNonEmptyPhones(data.showPhoneLists.get(idx));fn=sanitize(show+"_numbers.csv");csv.append("Name,Number\n");int ct=1;for(String p:ph){String nm=ct<10?"0"+ct:String.valueOf(ct);csv.append(escapeCsv(show+" "+nm)).append(",").append(escapeCsv(p)).append("\n");ct++;}}
        else if(kind.equals("categoryContacts")){String cat=normalizeCategory(getQueryParam(query,"cat"));if(cat==null){send404(ex);return;}fn=sanitize(cat+"_emails.csv");List<String>ovS=new ArrayList<>(),ovC=new ArrayList<>();loadCategoryOverrides(folder,ovS,ovC);csv.append("Email\n");for(int i=0;i<data.contactEmails.size();i++){boolean in=false;for(String s:data.contactShows.get(i))if(categoryForShow(s,ovS,ovC).equalsIgnoreCase(cat)){in=true;break;}if(in)csv.append(escapeCsv(data.contactEmails.get(i))).append("\n");}}
        else if(kind.equals("categoryPhones")){String cat=normalizeCategory(getQueryParam(query,"cat"));if(cat==null){send404(ex);return;}fn=sanitize(cat+"_numbers.csv");List<String>ph=uniqueNonEmptyPhones(getCategoryPhones(data,cat));csv.append("Name,Number\n");int ct=1;for(String p:ph){String nm=ct<10?"0"+ct:String.valueOf(ct);csv.append(escapeCsv(cat+" "+nm)).append(",").append(escapeCsv(p)).append("\n");ct++;}}
        else{send404(ex);return;}
        byte[]bytes=csv.toString().getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type","text/csv; charset=UTF-8");ex.getResponseHeaders().set("Content-Disposition","attachment; filename=\""+fn+"\"");
        ex.sendResponseHeaders(200,bytes.length);try(OutputStream os=ex.getResponseBody()){os.write(bytes);}
    }

    private static void handleUpload(HttpExchange ex) throws IOException {
        if(!"POST".equalsIgnoreCase(ex.getRequestMethod())){ex.getResponseHeaders().set("Location","/");ex.sendResponseHeaders(302,-1);ex.close();return;}
        UploadResult result = receiveCsvUpload(ex);
        if (result.error == null) {
            ex.getResponseHeaders().set("Location", "/?upload=success");
            ex.sendResponseHeaders(303, -1);
            ex.close();
        } else {
            sendHtml(ex, result.status, "<html><body><p>" + escapeHtml(result.error) + "</p><a href='/'>Back</a></body></html>");
        }
    }

    private static UploadResult receiveCsvUpload(HttpExchange ex) throws IOException {
        String contentType = ex.getRequestHeaders().getFirst("Content-Type");
        if (contentType == null || !contentType.toLowerCase(Locale.ROOT).contains("multipart/form-data")) {
            return UploadResult.failure(400, "Choose a CSV file to upload.");
        }
        String boundary = null;
        for (String contentTypePart : contentType.split(";")) {
            String trimmed = contentTypePart.trim();
            if (!trimmed.startsWith("boundary=")) continue;
            boundary = trimmed.substring(9);
            if (boundary.startsWith("\"") && boundary.endsWith("\"") && boundary.length() > 1) {
                boundary = boundary.substring(1, boundary.length() - 1);
            }
        }
        if (boundary == null || boundary.isEmpty()) return UploadResult.failure(400, "The upload boundary is missing.");

        final int maxUploadBytes = 25 * 1024 * 1024;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (InputStream input = ex.getRequestBody()) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = input.read(buffer)) != -1) {
                if (bytes.size() + read > maxUploadBytes) return UploadResult.failure(413, "Upload is too large (25 MB maximum).");
                bytes.write(buffer, 0, read);
            }
        }

        String body = new String(bytes.toByteArray(), StandardCharsets.ISO_8859_1);
        for (String part : body.split("--" + java.util.regex.Pattern.quote(boundary))) {
            if (part.trim().isEmpty() || part.startsWith("--")) continue;
            int headerEnd = part.indexOf("\r\n\r\n");
            if (headerEnd < 0) continue;
            String headerBlock = part.substring(0, headerEnd);
            String dataBlock = part.substring(headerEnd + 4);
            int dataEnd = dataBlock.lastIndexOf("\r\n");
            if (dataEnd >= 0) dataBlock = dataBlock.substring(0, dataEnd);
            String disposition = null;
            for (String header : headerBlock.split("\r\n")) {
                if (header.toLowerCase(Locale.ROOT).startsWith("content-disposition")) disposition = header;
            }
            if (disposition == null || !disposition.contains("name=\"csvFile\"")) continue;
            String fileName = "uploaded_" + System.currentTimeMillis() + ".csv";
            int fileNameIndex = disposition.toLowerCase(Locale.ROOT).indexOf("filename=");
            if (fileNameIndex >= 0) {
                String submittedName = disposition.substring(fileNameIndex + 9).trim();
                if (submittedName.startsWith("\"") && submittedName.endsWith("\"") && submittedName.length() > 1) {
                    submittedName = submittedName.substring(1, submittedName.length() - 1);
                }
                if (!submittedName.isEmpty()) fileName = submittedName;
            }
            fileName = fileName.replace('\\', '/');
            int slash = fileName.lastIndexOf('/');
            if (slash >= 0) fileName = fileName.substring(slash + 1);
            fileName = sanitize(fileName);
            if (fileName.isEmpty() || ".csv".equals(fileName)) fileName = "uploaded_" + System.currentTimeMillis() + ".csv";
            if (!fileName.toLowerCase(Locale.ROOT).endsWith(".csv")) fileName += ".csv";
            try (FileOutputStream output = new FileOutputStream(new File(".", fileName))) {
                output.write(dataBlock.getBytes(StandardCharsets.ISO_8859_1));
            }
            invalidateDataCache();
            return UploadResult.success(fileName);
        }
        return UploadResult.failure(400, "No CSV file was found in the upload.");
    }

    private static void handleExclusions(HttpExchange ex) throws IOException {
        File folder=new File(".");
        if("POST".equalsIgnoreCase(ex.getRequestMethod())){
            String body=readBody(ex),text=getFormParam(body,"emailsText");if(text==null)text="";
            List<String>lines=new ArrayList<>();
            try(BufferedReader br=new BufferedReader(new StringReader(text))){String ln;while((ln=br.readLine())!=null){String em=ln.trim().toLowerCase();if(!em.isEmpty()&&!containsString(lines,em))lines.add(em);}}
            try(BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(folder,"excluded_emails.txt")),StandardCharsets.UTF_8))){for(String em:lines){bw.write(em);bw.newLine();}}
            invalidateDataCache();
        }
        List<String> excl=loadExcludedEmails(folder);
        StringBuilder html=new StringBuilder();
        html.append("<!DOCTYPE html><html><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Excluded emails</title>").append(sharedCss()).append(sharedUiCss()).append("</head><body><div class='wrapper'>");
        html.append("<div class='hero'><div class='hero-pill'>Settings</div><h1 class='hero-title'>Excluded emails</h1><p class='hero-meta'>").append(excl.size()).append(" excluded</p></div>");
        html.append("<div class='card'><form method='post' action='/exclusions'><p class='tagline'>One email per line. Saved in <code>excluded_emails.txt</code>.</p><textarea name='emailsText'>");
        for(String em:excl)html.append(escapeHtml(em)).append("\n");
        html.append("</textarea><br><br><button type='submit'>Save</button></form><p><a href='/'>&#8592; Back</a></p></div></div></body></html>");
        sendHtml(ex,html.toString());
    }

    private static void handleCategories(HttpExchange ex) throws IOException {
        File folder=new File(".");
        if("POST".equalsIgnoreCase(ex.getRequestMethod())){
            String body=readBody(ex);int total=0;
            try{String t=getFormParam(body,"totalShows");if(t!=null)total=Integer.parseInt(t);}catch(Exception ignored){}
            List<String>outS=new ArrayList<>(),outC=new ArrayList<>();
            for(int i=0;i<total;i++){String show=getFormParam(body,"show_"+i),cat=getFormParam(body,"cat_"+i);if(show==null)continue;show=show.trim();if(cat==null)cat="";cat=cat.trim();if(!show.isEmpty()&&!cat.isEmpty()){outS.add(show);outC.add(cat);}}
            try(BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(folder,"categories.txt")),StandardCharsets.UTF_8))){for(int i=0;i<outS.size();i++){bw.write(escapeCsv(outS.get(i)));bw.write(",");bw.write(escapeCsv(outC.get(i)));bw.newLine();}}
            invalidateDataCache();
        }
        ProcessedData data=processFolder(folder);
        List<String>ovS=new ArrayList<>(),ovC=new ArrayList<>();loadCategoryOverrides(folder,ovS,ovC);
        StringBuilder html=new StringBuilder();
        html.append("<!DOCTYPE html><html><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Categories</title>").append(sharedCss()).append(sharedUiCss()).append("</head><body><div class='wrapper'>");
        html.append("<div class='hero'><div class='hero-pill'>Settings</div><h1 class='hero-title'>Show categories</h1></div>");
        html.append("<div class='card'><form method='post' action='/categories'><input type='hidden' name='totalShows' value='").append(data.showNames.size()).append("'>");
        html.append("<table><thead><tr><th>#</th><th>Show</th><th>Current</th><th>Override</th></tr></thead><tbody>");
        for(int i=0;i<data.showNames.size();i++){
            String show=data.showNames.get(i),guess=categoryGuess(show),override="";
            for(int oi=0;oi<ovS.size();oi++)if(ovS.get(oi).equalsIgnoreCase(show)){String normalized=normalizeCategory(ovC.get(oi));if(normalized!=null)override=normalized;break;}
            String cur=override.isEmpty()?guess:override;
            html.append("<tr><td>").append(i+1).append("</td><td>").append(escapeHtml(show)).append("</td><td>").append(escapeHtml(cur)).append("</td><td>");
            html.append("<input type='hidden' name='show_").append(i).append("' value='").append(escapeHtmlAttr(show)).append("'>");
            html.append("<select name='cat_").append(i).append("'><option value=''");if(override.isEmpty())html.append(" selected");html.append(">Auto (").append(escapeHtml(guess)).append(")</option>");
            for(String c:CATEGORIES){html.append("<option value='").append(escapeHtmlAttr(c)).append("'");if(override.equalsIgnoreCase(c))html.append(" selected");html.append(">").append(escapeHtml(c)).append("</option>");}
            html.append("</select></td></tr>");
        }
        html.append("</tbody></table><br><button type='submit'>Save</button></form><p><a href='/'>&#8592; Back</a></p></div></div></body></html>");
        sendHtml(ex,html.toString());
    }

    // ================================================================
    //  CORE DATA PROCESSING
    // ================================================================

    private static ProcessedData processFolder(File folder) {
        String revision=dataRevision(folder);
        ProcessedData cached=cachedProcessedData;
        if(cached!=null&&revision.equals(cachedDataRevision))return cached;
        synchronized(DATA_CACHE_LOCK){
            revision=dataRevision(folder);
            if(cachedProcessedData!=null&&revision.equals(cachedDataRevision))return cachedProcessedData;
            ProcessedData loaded=loadFolder(folder);
            cachedProcessedData=loaded;cachedDataRevision=revision;
            return loaded;
        }
    }

    private static void invalidateDataCache(){
        synchronized(DATA_CACHE_LOCK){cachedProcessedData=null;cachedDataRevision=null;}
    }

    private static String dataRevision(File folder){
        StringBuilder revision=new StringBuilder(folder.getAbsolutePath());
        File[]inputs=folder.listFiles((d,n)->(n.toLowerCase().endsWith(".csv")
                &&!n.equalsIgnoreCase("contacts_with_shows.csv")&&!n.equalsIgnoreCase("all_contacts.csv"))
                ||n.equalsIgnoreCase("categories.txt")||n.equalsIgnoreCase("excluded_emails.txt"));
        if(inputs==null)return revision.toString();
        Arrays.sort(inputs,(a,b)->a.getName().compareToIgnoreCase(b.getName()));
        for(File input:inputs)revision.append('|').append(input.getName()).append(':').append(input.length()).append(':').append(input.lastModified());
        return revision.toString();
    }

    private static ProcessedData loadFolder(File folder) {
        ProcessedData data = new ProcessedData();
        data.showNames=new ArrayList<>();data.showEmailsLists=new ArrayList<>();data.showPhoneLists=new ArrayList<>();
        data.catAfroEmails=new ArrayList<>();data.catJazzEmails=new ArrayList<>();data.catComedyEmails=new ArrayList<>();
        data.catPoetryEmails=new ArrayList<>();data.catHipHopEmails=new ArrayList<>();data.catReggaeEmails=new ArrayList<>();
        data.catAfroPhones=new ArrayList<>();data.catJazzPhones=new ArrayList<>();data.catComedyPhones=new ArrayList<>();
        data.catPoetryPhones=new ArrayList<>();data.catHipHopPhones=new ArrayList<>();data.catReggaePhones=new ArrayList<>();
        data.contactEmails=new ArrayList<>();data.contactPhones=new ArrayList<>();data.contactShows=new ArrayList<>();
        data.contactFirstNames=new ArrayList<>();data.contactSurnames=new ArrayList<>();
        data.showTicketCounts=new ArrayList<>();data.catTicketCounts=new int[6];
        data.totalTickets=0;data.timeBuckets=new int[4];data.groupSizeBuckets=new int[7];

        List<String> contactKeys=new ArrayList<>(),excluded=loadExcludedEmails(folder);
        List<String> ovS=new ArrayList<>(),ovC=new ArrayList<>();loadCategoryOverrides(folder,ovS,ovC);
        List<String> orderKeys=new ArrayList<>();List<Integer> orderCounts=new ArrayList<>();

        File[] csvs=folder.listFiles((d,n)->n.toLowerCase().endsWith(".csv")&&!n.equalsIgnoreCase("contacts_with_shows.csv")&&!n.equalsIgnoreCase("all_contacts.csv"));
        if(csvs==null)csvs=new File[0];
        Arrays.sort(csvs,(a,b)->a.getName().compareToIgnoreCase(b.getName()));

        for(File f:csvs){
            String base=f.getName().substring(0,f.getName().length()-4);
            String catName=categoryForShow(base,ovS,ovC);int catIdx=categoryIndex(catName);
            data.showNames.add(base);
            List<String>showEmails=new ArrayList<>(),showPhones=new ArrayList<>();
            data.showEmailsLists.add(showEmails);data.showPhoneLists.add(showPhones);data.showTicketCounts.add(0);
            int showIndex=data.showNames.size()-1;
            try(BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(f),StandardCharsets.UTF_8))){
                String header=br.readLine();if(header==null)continue;
                char delimiter=detectDelimiter(header);
                List<String>cols=parseCsvLine(header,delimiter);
                int iEmail=findIndex(cols,"Email"),iPurch=findIndex(cols,"Purchaser Email"),iPhone=findIndexAny(cols,"Cellphone","Phone"),
                        iTicket=findIndexAny(cols,"Ticket Number","Barcode"),iOrder=findIndex(cols,"Order Number"),iPayRef=findIndex(cols,"Payment Reference"),
                        iFirst=findIndex(cols,"First name"),iSurname=findIndex(cols,"Surname");
                String line;int rowNumber=0;
                while((line=br.readLine())!=null){
                    rowNumber++;
                    if(line.trim().isEmpty())continue;
                    List<String>row=parseCsvLine(line,delimiter);
                    String email=cv(row,iEmail).toLowerCase(),pEmail=cv(row,iPurch).toLowerCase();
                    String phone=cv(row,iPhone).replaceAll("\\D",""),ticket=cv(row,iTicket);
                    if(phone.startsWith("27")&&phone.length()==11)phone="0"+phone.substring(2);
                    if((!email.isEmpty()&&containsString(excluded,email))||(!pEmail.isEmpty()&&containsString(excluded,pEmail)))continue;
                    String fe=!email.isEmpty()?email:pEmail;
                    String key=!email.isEmpty()?"email:"+email:!pEmail.isEmpty()?"email:"+pEmail:!phone.isEmpty()?"phone:"+phone:!ticket.isEmpty()?"ticket:"+base+":"+ticket:"row:"+base+":"+rowNumber;
                    int ki=indexOfString(contactKeys,key);
                    String firstName=iFirst>=0&&iFirst<row.size()?cv(row,iFirst):""; String surname=iSurname>=0&&iSurname<row.size()?cv(row,iSurname):"";
                    if(ki==-1){contactKeys.add(key);data.contactEmails.add(fe);data.contactPhones.add(phone);data.contactFirstNames.add(firstName);data.contactSurnames.add(surname);List<String>sl=new ArrayList<>();sl.add(base);data.contactShows.add(sl);}
                    else{if(!fe.isEmpty()&&(data.contactEmails.get(ki)==null||data.contactEmails.get(ki).isEmpty()))data.contactEmails.set(ki,fe);if(!phone.isEmpty()&&(data.contactPhones.get(ki)==null||data.contactPhones.get(ki).isEmpty()))data.contactPhones.set(ki,phone);List<String>sl=data.contactShows.get(ki);if(!containsString(sl,base))sl.add(base);}
                    if(!fe.isEmpty()&&!containsString(showEmails,fe))showEmails.add(fe);
                    if(!phone.isEmpty()&&!containsString(showPhones,phone))showPhones.add(phone);
                    if(!fe.isEmpty())addCatEmail(data,catIdx,fe);
                    if(!phone.isEmpty())addCatPhone(data,catIdx,phone);
                    data.totalTickets++;data.showTicketCounts.set(showIndex,data.showTicketCounts.get(showIndex)+1);
                    if(catIdx>=1&&catIdx<=6)data.catTicketCounts[catIdx-1]++;
                    String ok=cv(row,iPayRef);if(ok.isEmpty())ok=cv(row,iOrder);
                    if(!ok.isEmpty()){int oi=indexOfString(orderKeys,ok);if(oi==-1){orderKeys.add(ok);orderCounts.add(1);}else orderCounts.set(oi,orderCounts.get(oi)+1);}
                    for(String cell:row){if(cell==null)continue;String v=cell.trim();if(v.length()>=13&&v.substring(0,10).matches("\\d{4}-\\d{2}-\\d{2}")&&v.charAt(10)==' '){try{int hr=Integer.parseInt(v.substring(11,13));if(hr>=6&&hr<=11)data.timeBuckets[0]++;else if(hr>=12&&hr<=16)data.timeBuckets[1]++;else if(hr>=17&&hr<=21)data.timeBuckets[2]++;else data.timeBuckets[3]++;}catch(Exception ignored){}break;}}
                }
            }catch(IOException e){e.printStackTrace();}
        }
        for(int c:orderCounts){if(c<=0)continue;data.groupSizeBuckets[Math.min(c,6)]++;}
        writeContactsWithShows(new File(folder,"contacts_with_shows.csv"),data.contactEmails,data.contactPhones,data.contactShows);
        return data;
    }

    private static String cv(List<String> row, int idx) {
        if(idx<0||idx>=row.size())return"";String v=row.get(idx);return v==null?"":v.trim();
    }
    private static double parsePrice(String raw) {
        if(raw==null)return 0;
        String v=raw.trim().replaceAll("[^0-9,.-]","");if(v.isEmpty()||v.equals("-")||v.equals(".")||v.equals(","))return 0;
        int comma=v.lastIndexOf(','),dot=v.lastIndexOf('.');
        if(comma>=0&&dot>=0){if(comma>dot)v=v.replace(".","").replace(',','.');else v=v.replace(",","");}
        else if(comma>=0){int decimals=v.length()-comma-1;v=decimals==1||decimals==2?v.replace(',','.'):v.replace(",","");}
        try{return Double.parseDouble(v);}catch(NumberFormatException ignored){return 0;}
    }
    private static void addCatEmail(ProcessedData d, int ci, String e) {
        switch(ci){case 1:if(!containsString(d.catAfroEmails,e))d.catAfroEmails.add(e);break;case 2:if(!containsString(d.catJazzEmails,e))d.catJazzEmails.add(e);break;case 3:if(!containsString(d.catComedyEmails,e))d.catComedyEmails.add(e);break;case 4:if(!containsString(d.catPoetryEmails,e))d.catPoetryEmails.add(e);break;case 5:if(!containsString(d.catHipHopEmails,e))d.catHipHopEmails.add(e);break;case 6:if(!containsString(d.catReggaeEmails,e))d.catReggaeEmails.add(e);break;}
    }
    private static void addCatPhone(ProcessedData d, int ci, String p) {
        switch(ci){case 1:if(!containsString(d.catAfroPhones,p))d.catAfroPhones.add(p);break;case 2:if(!containsString(d.catJazzPhones,p))d.catJazzPhones.add(p);break;case 3:if(!containsString(d.catComedyPhones,p))d.catComedyPhones.add(p);break;case 4:if(!containsString(d.catPoetryPhones,p))d.catPoetryPhones.add(p);break;case 5:if(!containsString(d.catHipHopPhones,p))d.catHipHopPhones.add(p);break;case 6:if(!containsString(d.catReggaePhones,p))d.catReggaePhones.add(p);break;}
    }
    private static List<String> getCategoryPhones(ProcessedData d, String cat) {
        if("Afrosoul".equals(cat))return d.catAfroPhones;if("Jazz".equals(cat))return d.catJazzPhones;if("Comedy".equals(cat))return d.catComedyPhones;if("Folk".equals(cat))return d.catPoetryPhones;if("HipHop".equals(cat))return d.catHipHopPhones;return d.catReggaePhones;
    }
    private static List<String> getCategoryEmails(ProcessedData d, String cat) {
        if("Afrosoul".equals(cat))return d.catAfroEmails;if("Jazz".equals(cat))return d.catJazzEmails;if("Comedy".equals(cat))return d.catComedyEmails;if("Folk".equals(cat))return d.catPoetryEmails;if("HipHop".equals(cat))return d.catHipHopEmails;return d.catReggaeEmails;
    }
    private static boolean containsString(List<String> l,String k){for(String s:l)if(s.equals(k))return true;return false;}
    private static int indexOfString(List<String> l,String k){for(int i=0;i<l.size();i++)if(l.get(i).equals(k))return i;return -1;}
    private static int findIndex(List<String> h,String n){for(int i=0;i<h.size();i++)if(h.get(i).trim().equalsIgnoreCase(n))return i;return -1;}
    private static int findIndexAny(List<String> h,String...names){for(String n:names){int i=findIndex(h,n);if(i>=0)return i;}return -1;}
    private static char detectDelimiter(String line){
        int commas=0,semicolons=0;boolean inQ=false;
        for(int i=0;i<line.length();i++){char ch=line.charAt(i);if(ch=='"'){if(inQ&&i+1<line.length()&&line.charAt(i+1)=='"'){i++;}else inQ=!inQ;}else if(!inQ){if(ch==',')commas++;else if(ch==';')semicolons++;}}
        return semicolons>commas?';':',';
    }
    private static List<String> parseCsvLine(String line){return parseCsvLine(line,detectDelimiter(line));}
    private static List<String> parseCsvLine(String line,char delimiter){
        List<String>out=new ArrayList<>();StringBuilder cell=new StringBuilder();boolean inQ=false;
        for(int i=0;i<line.length();i++){char ch=line.charAt(i);if(inQ){if(ch=='"'){if(i+1<line.length()&&line.charAt(i+1)=='"'){cell.append('"');i++;}else inQ=false;}else cell.append(ch);}else{if(ch=='"')inQ=true;else if(ch==delimiter){out.add(cell.toString());cell.setLength(0);}else cell.append(ch);}}
        out.add(cell.toString());return out;
    }
    private static void writeContactsWithShows(File file,List<String>emails,List<String>phones,List<List<String>>shows){
        try(BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file),StandardCharsets.UTF_8))){
            bw.write("Email,Number,ShowsAttended\n");
            for(int i=0;i<emails.size();i++){List<String>sl=shows.get(i);StringBuilder sb=new StringBuilder();if(sl!=null)for(int j=0;j<sl.size();j++){if(j>0)sb.append(" | ");sb.append(sl.get(j));}bw.write(escapeCsv(emails.get(i))+","+escapeCsv(phones.get(i))+","+escapeCsv(sb.toString())+"\n");}
        }catch(IOException e){e.printStackTrace();}
    }
    private static List<String> loadExcludedEmails(File folder){
        List<String>list=new ArrayList<>();File f=new File(folder,"excluded_emails.txt");if(!f.exists())return list;
        try(BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(f),StandardCharsets.UTF_8))){String ln;while((ln=br.readLine())!=null){String em=ln.trim().toLowerCase();if(!em.isEmpty()&&!containsString(list,em))list.add(em);}}catch(IOException ignored){}return list;
    }
    private static void loadCategoryOverrides(File folder,List<String>shows,List<String>cats){
        File f=new File(folder,"categories.txt");if(!f.exists())return;
        try(BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(f),StandardCharsets.UTF_8))){String ln;while((ln=br.readLine())!=null){if(ln.trim().isEmpty())continue;List<String>cols=parseCsvLine(ln);if(cols.size()<2)continue;String s=cols.get(0).trim(),c=cols.get(1).trim();if(!s.isEmpty()&&!c.isEmpty()){shows.add(s);cats.add(c);}}}catch(IOException ignored){}
    }
    private static String categoryForShow(String base,List<String>ovS,List<String>ovC){
        for(int i=0;i<ovS.size();i++)if(ovS.get(i).equalsIgnoreCase(base)){String normalized=normalizeCategory(ovC.get(i));return normalized==null?categoryGuess(base):normalized;}return categoryGuess(base);
    }
    private static List<String> uniqueNonEmptyPhones(List<String>src){
        List<String>out=new ArrayList<>();for(String p:src){if(p==null)continue;String v=p.trim();if(!v.isEmpty()&&!containsString(out,v))out.add(v);}return out;
    }
    private static String categoryGuess(String name){
        String s=name.toLowerCase();
        if(s.contains("jazz")||s.contains("feya")||s.contains("tutu")||s.contains("pasha"))return"Jazz";
        if(s.contains("comedy")||s.contains("gumbi")||s.contains("robvan")||s.contains("rob van")||s.contains("tats"))return"Comedy";
        if(s.contains("poetry")||s.contains("poet")||s.contains("folk"))return"Folk";
        if(s.contains("hiphop")||s.contains("hip hop")||s.contains("urban")||s.contains("yahkeem")||s.contains("yakheem"))return"HipHop";
        if(s.contains("reggae")||s.contains("selassie")||s.contains("survivals")||s.contains("420")||s.contains("dub"))return"Reggae";
        if(s.contains("soul")||s.contains("afro")||s.contains("zamajobe")||s.contains("mxo")||s.contains("camagwini")||s.contains("zolani")||s.contains("bongeziwe")||s.contains("ntsika")||s.contains("asanda")||s.contains("brenda"))return"Afrosoul";
        return"Afrosoul";
    }
    private static int categoryIndex(String n){switch(n.toLowerCase()){case"afrosoul":return 1;case"jazz":return 2;case"comedy":return 3;case"folk":return 4;case"hiphop":return 5;case"reggae":return 6;default:return 1;}}
    private static String normalizeCategory(String cat){if(cat==null)return null;switch(cat.toLowerCase()){case"afrosoul":return"Afrosoul";case"jazz":return"Jazz";case"comedy":return"Comedy";case"poetry":case"folk":return"Folk";case"hiphop":return"HipHop";case"reggae":return"Reggae";default:return null;}}
    private static void addCategoryRow(StringBuilder html,String cat,List<String>emails,List<String>phones){
        int ec=emails.size(),pc=uniqueNonEmptyPhones(phones).size();
        html.append("<tr><td><strong style='color:#f2c46b'>").append(escapeHtml(cat)).append("</strong></td>")
                .append("<td>").append(ec).append("</td><td>").append(pc).append("</td><td>")
                .append("<a href='/categoryContacts?cat=").append(cat).append("'>Emails</a>");
        if(pc>0)html.append(" &middot; <a href='/categoryPhones?cat=").append(cat).append("'>Numbers</a>");
        html.append("</td></tr>");
    }
    private static void sendHtml(HttpExchange ex,String html)throws IOException{sendHtml(ex,200,html);}
    private static void sendHtml(HttpExchange ex,int status,String html)throws IOException{byte[]bytes=html.getBytes(StandardCharsets.UTF_8);ex.getResponseHeaders().set("Content-Type","text/html; charset=UTF-8");ex.sendResponseHeaders(status,bytes.length);try(OutputStream os=ex.getResponseBody()){os.write(bytes);}}
    private static void send404(HttpExchange ex)throws IOException{sendHtml(ex,404,"<html><body><p>Not found.</p><p><a href='/'>Back</a></p></body></html>");}
    private static String escapeHtml(String s){if(s==null)return"";return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;");}
    private static String escapeHtmlAttr(String s){return escapeHtml(s).replace("\"","&quot;").replace("'","&#39;");}
    private static String escapeCsv(String v){if(v==null)return"";if(v.contains(",")||v.contains("\"")||v.contains("\n")||v.contains("\r"))v="\""+v.replace("\"","\"\"")+"\"";return v;}
    private static String sanitize(String s){if(s==null)return"export.csv";return s.replaceAll("[^a-zA-Z0-9_\\-\\.]","_");}
    private static int parseIntQueryParam(String q,String n,int def){String v=getQueryParam(q,n);if(v==null)return def;try{return Integer.parseInt(v);}catch(Exception e){return def;}}
    private static String getQueryParam(String q,String n){if(q==null)return null;for(String p:q.split("&")){int eq=p.indexOf('=');if(eq<=0)continue;String k=p.substring(0,eq),v=p.substring(eq+1);if(k.equals(n)){try{return URLDecoder.decode(v,"UTF-8");}catch(Exception e){return v;}}}return null;}
    private static String readBody(HttpExchange ex)throws IOException{ByteArrayOutputStream b=new ByteArrayOutputStream();try(InputStream in=ex.getRequestBody()){byte[]buf=new byte[4096];int r;while((r=in.read(buf))!=-1)b.write(buf,0,r);}return new String(b.toByteArray(),StandardCharsets.UTF_8);}
    private static String getFormParam(String body,String name){if(body==null)return null;for(String p:body.split("&")){int eq=p.indexOf('=');if(eq<=0)continue;String k=p.substring(0,eq),v=p.substring(eq+1);if(k.equals(name)){try{return URLDecoder.decode(v,"UTF-8");}catch(Exception e){return v;}}}return null;}
    private static String formatCurrency(double v){Locale locale=new Locale.Builder().setLanguage("en").setRegion("ZA").build();return NumberFormat.getCurrencyInstance(locale).format(v);}

    // ── JS array helpers ──────────────────────────────────────────────
    private static String toJSStringArray(List<String>l){StringBuilder sb=new StringBuilder("[");for(int i=0;i<l.size();i++){sb.append(jsStr(l.get(i)));if(i<l.size()-1)sb.append(",");}sb.append("]");return sb.toString();}
    private static String toJSIntArray(int[]a){StringBuilder sb=new StringBuilder("[");for(int i=0;i<a.length;i++){sb.append(a[i]);if(i<a.length-1)sb.append(",");}sb.append("]");return sb.toString();}
    private static String toJSIntList(List<Integer>l){StringBuilder sb=new StringBuilder("[");for(int i=0;i<l.size();i++){sb.append(l.get(i)==null?0:l.get(i));if(i<l.size()-1)sb.append(",");}sb.append("]");return sb.toString();}
    private static String toJSDoubleArray(double[]a){StringBuilder sb=new StringBuilder("[");for(int i=0;i<a.length;i++){sb.append(String.format(Locale.ROOT,"%.2f",a[i]));if(i<a.length-1)sb.append(",");}sb.append("]");return sb.toString();}
    private static String toJSDoubleList(List<Double>l){StringBuilder sb=new StringBuilder("[");for(int i=0;i<l.size();i++){sb.append(String.format(Locale.ROOT,"%.2f",l.get(i)==null?0:l.get(i)));if(i<l.size()-1)sb.append(",");}sb.append("]");return sb.toString();}

    // ================================================================
    //  CSS
    // ================================================================

    private static String rootCss(){return
            "<style>" +
                    ":root{--gold:#f2c46b;--gold-dim:rgba(242,196,107,0.15);--gold-border:rgba(242,196,107,0.3);--bg:#0a0a0a;--surface:#111;--surface2:#181818;--border:rgba(255,255,255,0.08);--text:#f5f5f5;--text-muted:rgba(255,255,255,0.5);}" +
                    "*{box-sizing:border-box;}" +
                    "html{scroll-behavior:smooth;}body{margin:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:15px;line-height:1.6;background:radial-gradient(circle at 50% -20%,#251b0e 0,var(--bg) 34%);color:var(--text);min-height:100vh;}" +
                    ".skip-link{position:fixed;left:16px;top:-60px;z-index:1000;background:var(--gold);color:#000;padding:10px 14px;border-radius:8px;font-weight:700;transition:top .15s;}.skip-link:focus{top:12px;}" +
                    "a:focus-visible,button:focus-visible,input:focus-visible,select:focus-visible{outline:3px solid rgba(242,196,107,.5);outline-offset:2px;}" +
                    ".wrapper{max-width:1160px;margin:0 auto;padding:0 0 60px;}" +
                    // TOP NAVIGATION BAR
                    ".topbar{background:rgba(0,0,0,.9);backdrop-filter:blur(14px);border-bottom:1px solid var(--gold-border);padding:0 max(24px,calc((100vw - 1240px)/2));display:flex;align-items:center;justify-content:space-between;min-height:64px;position:sticky;top:0;z-index:100;}" +
                    ".topbar-brand{display:flex;align-items:center;gap:14px;}" +
                    ".topbar-logo{width:36px;height:36px;border-radius:8px;background:rgba(255,255,255,0.05);border:1px solid var(--gold-border);display:flex;align-items:center;justify-content:center;overflow:hidden;}" +
                    ".topbar-logo img{max-width:32px;max-height:32px;display:block;}" +
                    ".topbar-name{font-size:15px;font-weight:700;color:#fff;letter-spacing:0.06em;text-transform:uppercase;}" +
                    ".topbar-nav{display:flex;gap:4px;}" +
                    ".topbar-nav a{padding:8px 14px;border-radius:6px;font-size:13px;font-weight:500;color:var(--text-muted);text-decoration:none;transition:all 0.15s;}" +
                    ".topbar-nav a:hover{color:var(--gold);background:var(--gold-dim);}" +
                    ".topbar-nav a.active{color:var(--gold);background:var(--gold-dim);}" +
                    // PAGE CONTENT AREA
                    ".page{width:min(1240px,100%);margin:0 auto;padding:40px 32px 0;}" +
                    // PAGE HEADER
                    ".page-header{margin-bottom:24px;max-width:760px;}" +
                    ".eyebrow{font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:.14em;color:var(--gold);margin-bottom:4px;}" +
                    ".page-header h1{margin:0 0 6px;font-size:clamp(28px,4vw,42px);line-height:1.12;font-weight:750;color:#fff;letter-spacing:-0.03em;}" +
                    ".page-header p{margin:0;font-size:15px;color:var(--text-muted);}" +
                    // STAT STRIP (top KPIs)
                    ".stat-strip{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:1px;background:var(--border);border:1px solid var(--border);border-radius:12px;overflow:hidden;margin-bottom:28px;}" +
                    ".stat-strip-item{background:var(--surface);padding:20px 22px;}" +
                    ".stat-strip-val{font-size:26px;font-weight:700;color:var(--gold);letter-spacing:-0.02em;line-height:1;}" +
                    ".stat-strip-label{font-size:12px;color:var(--text-muted);margin-top:5px;text-transform:uppercase;letter-spacing:0.06em;}" +
                    // MAIN TWO-COLUMN LAYOUT
                    ".main-grid{display:grid;grid-template-columns:1fr 340px;gap:20px;align-items:start;}" +
                    // CARDS
                    ".card{background:linear-gradient(180deg,rgba(24,24,24,.98),rgba(15,15,15,.98));border:1px solid var(--border);border-radius:14px;overflow:hidden;margin-bottom:20px;box-shadow:0 18px 50px rgba(0,0,0,.22);}" +
                    ".card-header{padding:18px 22px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;}" +
                    ".card-header h2{margin:0;font-size:16px;font-weight:600;color:#fff;}" +
                    ".card-header p{margin:0;font-size:13px;color:var(--text-muted);}" +
                    ".card-body{padding:0 22px 20px;}" +
                    ".card-body-pad{padding:22px;}" +
                    // TABLE
                    ".table-scroll{width:100%;overflow-x:auto;overscroll-behavior-inline:contain;}" +
                    "table{width:100%;min-width:680px;border-collapse:collapse;font-size:14px;}" +
                    "th{padding:10px 12px;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.08em;color:var(--text-muted);background:var(--surface2);text-align:left;border-bottom:1px solid var(--border);position:sticky;top:0;z-index:2;}" +
                    "td{padding:12px 12px;border-bottom:1px solid rgba(255,255,255,0.04);vertical-align:middle;}" +
                    "tbody tr:last-child td{border-bottom:none;}" +
                    "tbody tr:hover td{background:var(--surface2);}.row-number{color:var(--text-muted);font-variant-numeric:tabular-nums;}" +
                    ".sort-button{appearance:none;background:transparent!important;color:inherit!important;border:0!important;border-radius:4px!important;padding:0!important;font:inherit!important;text-transform:inherit;letter-spacing:inherit;display:inline-flex;align-items:center;gap:5px;box-shadow:none!important;}.sort-button:hover{color:var(--gold)!important;}" +
                    ".table-tools{display:grid;grid-template-columns:minmax(220px,1fr) 190px auto;gap:12px;align-items:end;padding:16px 20px;border-bottom:1px solid var(--border);background:rgba(255,255,255,.015);}" +
                    ".field{display:flex;flex-direction:column;gap:6px;}.field>span{font-size:11px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--text-muted);}.field input,.field select{width:100%;height:42px;border:1px solid var(--border);border-radius:8px;background:#0d0d0d;color:var(--text);padding:0 12px;font-size:14px;}.field input::placeholder{color:rgba(255,255,255,.32);}.field input:focus,.field select:focus{border-color:var(--gold-border);}" +
                    ".result-count{justify-self:end;padding-bottom:10px;color:var(--text-muted);font-size:13px;white-space:nowrap;}" +
                    ".table-pagination{display:flex;align-items:center;justify-content:flex-end;gap:12px;padding:14px 20px;border-top:1px solid var(--border);font-size:13px;color:var(--text-muted);}.btn-small{padding:7px 12px;font-size:12px;}" +
                    ".empty-state{text-align:center;padding:44px 20px;color:var(--text-muted);}.empty-state strong{display:block;color:#fff;font-size:16px;margin-bottom:4px;}.empty-state p{margin:0;}.empty-state.compact{padding:24px 12px;}" +
                    // BADGE
                    ".badge{display:inline-block;padding:3px 10px;border-radius:999px;font-size:12px;font-weight:500;background:var(--gold-dim);border:1px solid var(--gold-border);color:var(--gold);}" +
                    // LINKS
                    "a{color:var(--gold);text-decoration:none;}a:hover{color:#ffe3a5;}" +
                    // BUTTONS
                    ".btn{display:inline-block;padding:10px 22px;border-radius:8px;font-size:14px;font-weight:600;cursor:pointer;border:none;transition:all 0.15s;}" +
                    ".btn-primary{background:var(--gold);color:#000;}.btn-primary:hover{background:#ffe3a5;}" +
                    ".btn-outline{background:transparent;color:var(--gold);border:1px solid var(--gold-border);}.btn-outline:hover{background:var(--gold-dim);}" +
                    "button{display:inline-block;padding:10px 22px;border-radius:8px;font-size:14px;font-weight:600;cursor:pointer;border:none;background:var(--gold);color:#000;transition:all 0.15s;}button:hover{background:#ffe3a5;}button:disabled{cursor:not-allowed;opacity:.45;}" +
                    // SIDEBAR CARDS
                    ".sidebar-card{background:linear-gradient(180deg,rgba(24,24,24,.98),rgba(15,15,15,.98));border:1px solid var(--border);border-radius:14px;padding:20px;margin-bottom:16px;box-shadow:0 18px 50px rgba(0,0,0,.18);}" +
                    ".sidebar-card h3{margin:0 0 12px;font-size:14px;font-weight:600;color:#fff;padding-bottom:10px;border-bottom:1px solid var(--border);}" +
                    ".sidebar-stat{display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid rgba(255,255,255,0.04);font-size:14px;}" +
                    ".sidebar-stat:last-child{border-bottom:none;}" +
                    ".sidebar-stat-label{color:var(--text-muted);}" +
                    ".sidebar-stat-val{font-weight:600;color:#fff;}" +
                    // ACTION BUTTONS ROW
                    ".action-buttons,.report-actions{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:20px;}.report-section-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;}" +
                    // SECTION DIVIDER
                    ".section-title{font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:0.1em;color:var(--text-muted);margin:28px 0 12px;padding-bottom:8px;border-bottom:1px solid var(--border);}" +
                    // UTILITY
                    ".muted{color:var(--text-muted);font-size:13px;}.sr-only{position:absolute!important;width:1px!important;height:1px!important;padding:0!important;margin:-1px!important;overflow:hidden!important;clip:rect(0,0,0,0)!important;white-space:nowrap!important;border:0!important;}[hidden]{display:none!important;}" +
                    "code{background:var(--surface2);border:1px solid var(--border);border-radius:4px;padding:1px 6px;font-size:12px;}" +
                    "input[type='file']{font-size:12px;color:var(--text-muted);width:100%;}.upload-drop{display:flex;flex-direction:column;gap:5px;padding:16px;margin-bottom:12px;border:1px dashed var(--gold-border);border-radius:10px;background:var(--gold-dim);cursor:pointer;}.upload-drop:hover{border-color:var(--gold);}.upload-title{font-weight:700;color:#fff;}.upload-drop #selectedFile{font-size:12px;color:var(--text-muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}.upload-drop input{margin-top:8px;}.form-status{min-height:20px;margin:8px 0 0;color:var(--text-muted);font-size:12px;}" +
                    "textarea{width:100%;min-height:180px;border-radius:8px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-size:13px;padding:12px;resize:vertical;}" +
                    "select{background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:6px 10px;font-size:13px;}" +
                    ".toast{margin-bottom:20px;padding:13px 16px;border-radius:10px;border:1px solid var(--border);font-size:14px;}.toast-success{color:#bff2d0;background:rgba(78,190,119,.12);border-color:rgba(78,190,119,.35);}" +
                    ".footer{padding:24px max(16px,calc((100vw - 1240px)/2));font-size:12px;color:var(--text-muted);border-top:1px solid var(--border);margin-top:40px;}" +
                    "@media(max-width:900px){.main-grid{grid-template-columns:1fr;}.page{padding:28px 16px 0;}.topbar{padding:10px 16px;gap:10px;align-items:flex-start;flex-direction:column;}.topbar-nav{width:100%;overflow-x:auto;padding-bottom:2px;}.topbar-nav a{white-space:nowrap;}.table-tools{grid-template-columns:1fr 180px;}.result-count{justify-self:start;padding:0;}.stat-strip{grid-template-columns:repeat(2,1fr);}}" +
                    "@media(max-width:560px){.topbar-name{font-size:13px;}.page-header h1{font-size:30px;}.table-tools{grid-template-columns:1fr;}.result-count{grid-column:1;}.stat-strip{grid-template-columns:1fr 1fr;}.stat-strip-item{padding:16px;}.stat-strip-val{font-size:22px;}.action-buttons .btn,.report-actions button{width:100%;text-align:center;}.report-section-grid{grid-template-columns:1fr;}.table-pagination{justify-content:space-between;}.card-header{align-items:flex-start;gap:12px;}.card-header p{text-align:right;} }" +
                    "</style>";
    }

    private static String rootDashboardScript(){return """
<script>
(() => {
  const table = document.getElementById('showsTable');
  if (table) {
    const body = document.getElementById('showsTableBody');
    const rows = Array.from(body.querySelectorAll('tr[data-name]'));
    const noResults = document.getElementById('noShowResults');
    const search = document.getElementById('showSearch');
    const category = document.getElementById('categoryFilter');
    const count = document.getElementById('showResultCount');
    const pageStatus = document.getElementById('showPageStatus');
    const previous = document.getElementById('previousShows');
    const next = document.getElementById('nextShows');
    const pageSize = 12;
    let page = 1;
    let sortKey = 'name';
    let ascending = true;

    function visibleRows() {
      const term = search.value.trim().toLocaleLowerCase();
      const selectedCategory = category.value;
      return rows.filter(row => {
        const matchesTerm = !term || row.dataset.name.includes(term) || row.dataset.category.includes(term);
        const matchesCategory = !selectedCategory || row.dataset.category === selectedCategory;
        return matchesTerm && matchesCategory;
      }).sort((a, b) => {
        if (sortKey === 'emails' || sortKey === 'phones') {
          return Number(a.dataset[sortKey]) - Number(b.dataset[sortKey]);
        }
        return a.dataset[sortKey].localeCompare(b.dataset[sortKey], undefined, {sensitivity:'base'});
      }).map((row, index, sorted) => ascending ? row : sorted[sorted.length - index - 1]);
    }

    function render() {
      const filtered = visibleRows();
      const pages = Math.max(1, Math.ceil(filtered.length / pageSize));
      page = Math.min(page, pages);
      const start = (page - 1) * pageSize;
      const current = filtered.slice(start, start + pageSize);
      rows.forEach(row => { row.hidden = true; body.appendChild(row); });
      current.forEach((row, index) => {
        row.hidden = false;
        row.querySelector('.row-number').textContent = String(start + index + 1);
        body.appendChild(row);
      });
      noResults.hidden = filtered.length !== 0;
      body.appendChild(noResults);
      count.textContent = `${filtered.length} of ${rows.length} shows`;
      pageStatus.textContent = `Page ${page} of ${pages}`;
      previous.disabled = page <= 1;
      next.disabled = page >= pages;
    }

    search.addEventListener('input', () => { page = 1; render(); });
    category.addEventListener('change', () => { page = 1; render(); });
    previous.addEventListener('click', () => { if (page > 1) { page--; render(); } });
    next.addEventListener('click', () => { const pages = Math.ceil(visibleRows().length / pageSize); if (page < pages) { page++; render(); } });
    table.querySelectorAll('[data-sort]').forEach(button => button.addEventListener('click', () => {
      const key = button.dataset.sort;
      if (sortKey === key) ascending = !ascending; else { sortKey = key; ascending = true; }
      page = 1;
      table.querySelectorAll('th[aria-sort]').forEach(th => th.removeAttribute('aria-sort'));
      button.closest('th').setAttribute('aria-sort', ascending ? 'ascending' : 'descending');
      table.querySelectorAll('[data-sort] span').forEach(icon => icon.textContent = '↕');
      button.querySelector('span').textContent = ascending ? '↑' : '↓';
      render();
    }));
    render();
  }

  const fileInput = document.getElementById('csvFile');
  const selectedFile = document.getElementById('selectedFile');
  if (fileInput) fileInput.addEventListener('change', () => {
    selectedFile.textContent = fileInput.files.length ? fileInput.files[0].name : 'No file selected';
  });
  const uploadForm = document.getElementById('uploadForm');
  if (uploadForm) uploadForm.addEventListener('submit', () => {
    const button = document.getElementById('uploadButton');
    button.disabled = true;
    button.textContent = 'Uploading…';
    document.getElementById('uploadFormStatus').textContent = 'Processing the CSV and refreshing audience data.';
  });
})();
</script>
""";}

    private static String sharedUiCss(){return """
<style>
:root{--gold:#f2c46b;--gold-dim:rgba(242,196,107,.14);--gold-border:rgba(242,196,107,.3);--surface:#111;--surface2:#181818;--border:rgba(255,255,255,.09);--text:#f5f5f5;--muted:rgba(255,255,255,.56)}
*{box-sizing:border-box}html{scroll-behavior:smooth}body{min-height:100vh;background:radial-gradient(circle at 50% -20%,#251b0e 0,#090909 35%);color:var(--text)}
.wrapper{max-width:1120px;padding:36px 24px 64px}.hero{border-radius:16px;border-color:var(--gold-border);box-shadow:0 20px 60px rgba(0,0,0,.32)}
.card{border-radius:14px;border-color:var(--border);box-shadow:0 18px 50px rgba(0,0,0,.2);overflow-x:auto}.card table{min-width:620px}
th{position:sticky;top:0;z-index:2;background:var(--surface2)}
a:focus-visible,button:focus-visible,textarea:focus-visible,select:focus-visible,input:focus-visible{outline:3px solid rgba(242,196,107,.5);outline-offset:2px}
button{border-radius:8px}.muted{color:var(--muted)}textarea,select{border-color:var(--border)}
@media(max-width:700px){.wrapper{padding:20px 14px 48px}.hero{padding:20px 16px}.hero-title{font-size:21px}.card{padding:14px}.card table{min-width:560px}th,td{padding:9px 8px}}
</style>
""";}

    // ================================================================
    //  REPORT GENERATOR – /report
    //  Browser-side Excel via SheetJS (no server dependency)
    // ================================================================

    private static void handleReport(HttpExchange ex) throws IOException {
        File folder = new File(".");
        ProcessedData data = processFolder(folder);
        List<String> ovS = new ArrayList<>(), ovC = new ArrayList<>();
        loadCategoryOverrides(folder, ovS, ovC);

        // run all engines so we have the data
        GraphAnalysis    graph  = runGraphAnalysis(data, ovS, ovC);
        StatResult       stats  = runStatistics(data, graph, folder);
        GreedyResult     greedy = runGreedy(data, graph, ovS, ovC);
        DPResult         dp     = runDP(data, graph);
        DemographicResult demo  = runDemographics(data);

        int C = data.contactEmails.size();
        int S = data.showNames.size();

        // compute retC/newC for demographics
        int retC2=0; for(List<String>sh:data.contactShows){if(sh!=null&&sh.size()>1)retC2++;}
        int newC2=C-retC2;

        Map<String,Double> reportYearRevenue=new TreeMap<>();
        double[] reportCategoryRevenue=new double[CATEGORIES.length];
        Map<Integer,Double> reportMonthRevenue=new TreeMap<>();
        Map<String,Double> reportSeasonRevenue=new LinkedHashMap<>();
        reportSeasonRevenue.put("Summer",0.0);reportSeasonRevenue.put("Autumn",0.0);
        reportSeasonRevenue.put("Winter",0.0);reportSeasonRevenue.put("Spring",0.0);
        File[] reportCsvs=folder.listFiles((d,n)->n.toLowerCase().endsWith(".csv")
                &&!n.equalsIgnoreCase("contacts_with_shows.csv")&&!n.equalsIgnoreCase("all_contacts.csv"));
        if(reportCsvs==null)reportCsvs=new File[0];
        for(File f:reportCsvs){
            String base=f.getName().substring(0,f.getName().length()-4);
            int category=categoryIndex(categoryForShow(base,ovS,ovC))-1;
            try(BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(f),StandardCharsets.UTF_8))){
                String header=br.readLine();if(header==null)continue;char delimiter=detectDelimiter(header);
                List<String>cols=parseCsvLine(header,delimiter);
                int iPrice=findIndex(cols,"Price paid"),iPurchase=findIndex(cols,"Purchase Date"),iEvent=findIndex(cols,"EventDate");
                String line;while((line=br.readLine())!=null){
                    if(line.trim().isEmpty())continue;List<String>row=parseCsvLine(line,delimiter);
                    double price=iPrice>=0&&iPrice<row.size()?parsePrice(row.get(iPrice)):0;
                    if(category>=0&&category<reportCategoryRevenue.length)reportCategoryRevenue[category]+=price;
                    String purchase=cv(row,iPurchase);if(purchase.matches("\\d{4}.*"))reportYearRevenue.merge(purchase.substring(0,4),price,Double::sum);
                    String event=cv(row,iEvent);if(!event.isEmpty())try{
                        LocalDate date=LocalDate.parse(event.split(" ")[0]);int month=date.getMonthValue();
                        reportMonthRevenue.merge(month,price,Double::sum);
                        String season=(month==12||month<=2)?"Summer":month<=5?"Autumn":month<=8?"Winter":"Spring";
                        reportSeasonRevenue.merge(season,price,Double::sum);
                    }catch(Exception ignored){}
                }
            }catch(IOException e){e.printStackTrace();}
        }

        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html><html><head><meta charset='UTF-8'><meta name='viewport' content='width=device-width,initial-scale=1'>");
        html.append("<title>Generate Report - The One Room</title>");
        html.append(rootCss());
        html.append("<script src='https://cdn.jsdelivr.net/npm/xlsx@0.18.5/dist/xlsx.full.min.js'></script>");
        html.append("</head><body>");

        // TOP NAV
        html.append("<nav class='topbar'>");
        html.append("<div class='topbar-brand'><div class='topbar-logo'><img src='/logo' alt='logo'></div><span class='topbar-name'>The One Room</span></div>");
        html.append("<div class='topbar-nav'>");
        html.append("<a href='/'>Dashboard</a><a href='/dataInsights'>Insights</a><a href='/report' class='active'>Reports</a><a href='/exclusions'>Settings</a>");
        html.append("</div></nav>");

        html.append("<div class='page'>");
        html.append("<div class='page-header' style='margin-top:28px'>");
        html.append("<h1>Generate Report</h1>");
        html.append("<p>Choose the sections you want to include, then click Generate. The report downloads as an Excel file.</p>");
        html.append("</div>");

        html.append("<div class='main-grid'>");
        html.append("<div>");

        // SECTION CHECKBOXES
        html.append("<div class='card'>");
        html.append("<div class='card-header'><h2>Select sections to include</h2></div>");
        html.append("<div class='card-body-pad'>");
        html.append("<div class='report-section-grid'>");

        String[] sections = {
                "contacts",   "All Contacts (email, phone, shows attended)",
                "shows",      "Shows (ticket counts, unique audience per show)",
                "categories", "Category Summary (reach per genre)",
                "revenue",    "Revenue by Year and Category",
                "retention",  "Audience Retention (new vs returning)",
                "time",       "Booking Time of Day",
                "groups",     "Group Buying Patterns",
                "seasons",    "Seasonality (revenue by month and season)",
                "graph",      "Audience Connections (Jaccard overlap, bridgeness)",
                "outliers",   "Show Outliers (z-score analysis)",
                "greedy",     "Reach Optimisation (set cover results)",
                "schedule",   "Recommended Schedule (knapsack planning)",
                "gender",     "Audience Gender Estimate",
                "background", "Audience Cultural Background Estimate"
        };

        for (int i = 0; i < sections.length; i += 2) {
            String id    = sections[i];
            String label = sections[i+1];
            html.append("<label style='display:flex;align-items:flex-start;gap:10px;padding:14px;background:var(--surface2);border-radius:8px;border:1px solid var(--border);cursor:pointer'>");
            html.append("<input type='checkbox' id='chk_").append(id).append("' checked style='width:16px;height:16px;margin-top:2px;accent-color:var(--gold)'>");
            html.append("<div><div style='font-weight:600;font-size:14px;color:#fff'>").append(label.split("[(]")[0].trim()).append("</div>");
            if (label.contains("(")) {
                html.append("<div style='font-size:12px;color:var(--text-muted);margin-top:2px'>").append(label.replaceAll(".*[(]","").replace(")","")).append("</div>");
            }
            html.append("</div></label>");
        }

        html.append("</div>"); // grid
        html.append("<div class='report-actions'>");
        html.append("<button id='generateReportButton' onclick='generateReport()' class='btn btn-primary' style='font-size:15px;padding:12px 28px'>Generate Excel Report</button>");
        html.append("<button onclick='selectAll()' style='background:transparent;color:var(--gold);border:1px solid var(--gold-border);'>Select all</button>");
        html.append("<button onclick='selectNone()' style='background:transparent;color:var(--text-muted);border:1px solid var(--border)'>Clear all</button>");
        html.append("</div>");
        html.append("<p id='reportStatus' class='form-status' role='status' aria-live='polite'></p>");
        html.append("</div>"); // card-body-pad
        html.append("</div>"); // card

        html.append("</div>"); // left col

        // RIGHT: PREVIEW SIDEBAR
        html.append("<div>");
        html.append("<div class='sidebar-card'>");
        html.append("<h3>Report preview</h3>");
        html.append("<div class='sidebar-stat'><span class='sidebar-stat-label'>Total contacts</span><span class='sidebar-stat-val'>").append(C).append("</span></div>");
        html.append("<div class='sidebar-stat'><span class='sidebar-stat-label'>Shows</span><span class='sidebar-stat-val'>").append(S).append("</span></div>");
        html.append("<div class='sidebar-stat'><span class='sidebar-stat-label'>Total tickets</span><span class='sidebar-stat-val'>").append(data.totalTickets).append("</span></div>");
        html.append("<div class='sidebar-stat'><span class='sidebar-stat-label'>Format</span><span class='sidebar-stat-val'>.xlsx</span></div>");
        html.append("<p style='font-size:12px;color:var(--text-muted);margin-top:12px'>Each selected section becomes its own tab in the Excel workbook. Data is current as of now.</p>");
        html.append("</div>");
        html.append("</div>"); // sidebar
        html.append("</div>"); // main-grid

        html.append("</div>"); // page
        html.append("<div class='footer'>The One Room Music and Comedy Club</div>");

        // ── JAVASCRIPT DATA + XLSX GENERATION ─────────────────────
        html.append("<script>");

        // embed all data as JS variables
        html.append("const DATA={");

        // contacts
        html.append("contacts:[['Email','Phone','Shows Attended','First Name','Surname'],");
        for (int i = 0; i < C; i++) {
            String email = data.contactEmails.get(i) == null ? "" : data.contactEmails.get(i);
            String phone = data.contactPhones.get(i) == null ? "" : data.contactPhones.get(i);
            List<String> shows = data.contactShows.get(i);
            String showsStr = shows == null ? "" : String.join(" | ", shows);
            String fn = data.contactFirstNames.size() > i ? data.contactFirstNames.get(i) : "";
            String sn = data.contactSurnames.size() > i ? data.contactSurnames.get(i) : "";
            html.append("[").append(jsStr(email)).append(",").append(jsStr(phone)).append(",")
                    .append(jsStr(showsStr)).append(",").append(jsStr(fn)).append(",").append(jsStr(sn)).append("],");
        }
        html.append("],");

        // shows
        html.append("shows:[['Show','Category','Tickets Sold','Unique Contacts'],");
        for (int i = 0; i < S; i++) {
            String show = data.showNames.get(i);
            String cat  = categoryForShow(show, ovS, ovC);
            int tickets = data.showTicketCounts.get(i);
            int unique  = graph.showToContacts.get(i).size();
            html.append("[").append(jsStr(show)).append(",").append(jsStr(cat)).append(",")
                    .append(tickets).append(",").append(unique).append("],");
        }
        html.append("],");

        // categories
        String[] catNames = {"Afrosoul","Jazz","Comedy","Folk","HipHop","Reggae"};
        int[]    catEmails = {
                data.catAfroEmails.size(), data.catJazzEmails.size(), data.catComedyEmails.size(),
                data.catPoetryEmails.size(), data.catHipHopEmails.size(), data.catReggaeEmails.size()
        };
        html.append("categories:[['Category','Unique Emails','Unique Numbers'],");
        for (int i = 0; i < 6; i++) {
            List<String> phones;
            switch(i){case 0:phones=data.catAfroPhones;break;case 1:phones=data.catJazzPhones;break;case 2:phones=data.catComedyPhones;break;case 3:phones=data.catPoetryPhones;break;case 4:phones=data.catHipHopPhones;break;default:phones=data.catReggaePhones;}
            html.append("[").append(jsStr(catNames[i])).append(",").append(catEmails[i]).append(",")
                    .append(uniqueNonEmptyPhones(phones).size()).append("],");
        }
        html.append("],");

        // revenue by year and category
        html.append("revenue:[['Breakdown','Period / Category','Revenue (ZAR)'],");
        for(Map.Entry<String,Double>entry:reportYearRevenue.entrySet())
            html.append("['Year',").append(jsStr(entry.getKey())).append(",")
                    .append(String.format(Locale.ROOT,"%.2f",entry.getValue())).append("],");
        for(int i=0;i<CATEGORIES.length;i++)
            html.append("['Category',").append(jsStr(CATEGORIES[i])).append(",")
                    .append(String.format(Locale.ROOT,"%.2f",reportCategoryRevenue[i])).append("],");
        html.append("],");

        // seasonality by calendar month and South African season
        String[] monthNames={"January","February","March","April","May","June","July","August","September","October","November","December"};
        html.append("seasons:[['Breakdown','Period','Revenue (ZAR)'],");
        for(int month=1;month<=12;month++)
            html.append("['Month',").append(jsStr(monthNames[month-1])).append(",")
                    .append(String.format(Locale.ROOT,"%.2f",reportMonthRevenue.getOrDefault(month,0.0))).append("],");
        for(Map.Entry<String,Double>entry:reportSeasonRevenue.entrySet())
            html.append("['Season',").append(jsStr(entry.getKey())).append(",")
                    .append(String.format(Locale.ROOT,"%.2f",entry.getValue())).append("],");
        html.append("],");

        // retention
        html.append("retention:[['Segment','Contacts','Percentage'],");
        html.append("['New (attended once)',").append(newC2).append(",").append(C==0?0:Math.round(newC2*100.0/C)).append("],");
        html.append("['Returning (2+ shows)',").append(retC2).append(",").append(C==0?0:Math.round(retC2*100.0/C)).append("],");
        html.append("],");

        // time of day
        String[] timeLabels = {"Morning (6am-11am)","Afternoon (12pm-4pm)","Evening (5pm-9pm)","Late / Other"};
        int timeTotal = 0; for(int v:data.timeBuckets) timeTotal+=v;
        html.append("time:[['Time Window','Bookings','Percentage'],");
        for (int i = 0; i < 4; i++) {
            int pct = timeTotal==0?0:Math.round(data.timeBuckets[i]*100.0f/timeTotal);
            html.append("[").append(jsStr(timeLabels[i])).append(",").append(data.timeBuckets[i]).append(",").append(pct).append("],");
        }
        html.append("],");

        // group sizes
        html.append("groups:[['Group Size','Orders'],");
        String[] gLabels = {"","1 ticket","2 tickets","3 tickets","4 tickets","5 tickets","6+ tickets"};
        for (int i = 1; i <= 6; i++) {
            html.append("[").append(jsStr(gLabels[i])).append(",").append(data.groupSizeBuckets[i]).append("],");
        }
        html.append("],");

        // graph / jaccard
        html.append("graph:[['Show A','Show B','Jaccard Similarity','Shared Audience'],");
        for (String[] pair : stats.topJaccardPairs) {
            int shared = 0;
            int a = indexOfString(data.showNames, pair[0]), b = indexOfString(data.showNames, pair[1]);
            if (a>=0&&b>=0) for(int ci:graph.showToContacts.get(a)) if(graph.showToContacts.get(b).contains(ci)) shared++;
            html.append("[").append(jsStr(pair[0])).append(",").append(jsStr(pair[1])).append(",")
                    .append(pair[2]).append(",").append(shared).append("],");
        }
        html.append("],");

        // outliers / z-scores
        html.append("outliers:[['Show','Tickets','Z-Score','Flag'],");
        for (int i = 0; i < S; i++) {
            double z = stats.showZScores == null || stats.showZScores.length <= i ? 0 : stats.showZScores[i];
            String flag = Math.abs(z) > 2 ? "Outlier" : "Normal";
            html.append("[").append(jsStr(data.showNames.get(i))).append(",")
                    .append(data.showTicketCounts.get(i)).append(",")
                    .append(String.format(Locale.ROOT,"%.2f",z)).append(",").append(jsStr(flag)).append("],");
        }
        html.append("],");

        // greedy
        html.append("greedy:[['Step','Show','New Contacts Covered','Cumulative Coverage %'],");
        int cum = 0;
        for (int i = 0; i < greedy.coverSet.size(); i++) {
            cum += greedy.coverMarginal.get(i);
            double pct = greedy.totalContacts==0?0:cum*100.0/greedy.totalContacts;
            html.append("[").append(i+1).append(",").append(jsStr(greedy.coverSet.get(i))).append(",")
                    .append(greedy.coverMarginal.get(i)).append(",").append(String.format(Locale.ROOT,"%.1f",pct)).append("],");
        }
        html.append("],");

        // schedule / knapsack
        html.append("schedule:[['Show','Included in Optimal Schedule'],");
        for (int i = 0; i < S; i++) {
            boolean included = dp.optimalShows.contains(data.showNames.get(i));
            html.append("[").append(jsStr(data.showNames.get(i))).append(",")
                    .append(jsStr(included ? "Yes" : "No")).append("],");
        }
        html.append("],");

        // gender
        int gTotal = demo.genderMale + demo.genderFemale + demo.genderUnknown;
        html.append("gender:[['Gender','Contacts','Percentage'],");
        html.append("['Male',").append(demo.genderMale).append(",").append(gTotal==0?0:Math.round(demo.genderMale*100.0/gTotal)).append("],");
        html.append("['Female',").append(demo.genderFemale).append(",").append(gTotal==0?0:Math.round(demo.genderFemale*100.0/gTotal)).append("],");
        html.append("['Unknown',").append(demo.genderUnknown).append(",").append(gTotal==0?0:Math.round(demo.genderUnknown*100.0/gTotal)).append("],");
        html.append("],");

        // cultural background
        int bgTotal = demo.bgBlackAfrican+demo.bgAfrikaans+demo.bgEnglish+demo.bgAsian+demo.bgOther+demo.bgUnknown;
        html.append("background:[['Cultural Background','Contacts','Percentage (estimate)'],");
        html.append("['Black African',").append(demo.bgBlackAfrican).append(",").append(bgTotal==0?0:Math.round(demo.bgBlackAfrican*100.0/bgTotal)).append("],");
        html.append("['Afrikaans',").append(demo.bgAfrikaans).append(",").append(bgTotal==0?0:Math.round(demo.bgAfrikaans*100.0/bgTotal)).append("],");
        html.append("['English',").append(demo.bgEnglish).append(",").append(bgTotal==0?0:Math.round(demo.bgEnglish*100.0/bgTotal)).append("],");
        html.append("['Asian',").append(demo.bgAsian).append(",").append(bgTotal==0?0:Math.round(demo.bgAsian*100.0/bgTotal)).append("],");
        html.append("['Other',").append(demo.bgOther).append(",").append(bgTotal==0?0:Math.round(demo.bgOther*100.0/bgTotal)).append("],");
        html.append("['Unknown',").append(demo.bgUnknown).append(",").append(bgTotal==0?0:Math.round(demo.bgUnknown*100.0/bgTotal)).append("],");
        html.append("],");

        html.append("};"); // end DATA

        // ── XLSX generation function ──
        html.append("""
function isChecked(id){return document.getElementById('chk_'+id)&&document.getElementById('chk_'+id).checked;}
function selectAll(){document.querySelectorAll('input[type=checkbox]').forEach(c=>c.checked=true);}
function selectNone(){document.querySelectorAll('input[type=checkbox]').forEach(c=>c.checked=false);}
function styleSheet(ws){
  const gold='FF f2c46b'.replace(/\s/g,'');
  const black='FF000000';
  const range=XLSX.utils.decode_range(ws['!ref']);
  for(let C2=range.s.c;C2<=range.e.c;C2++){
    const addr=XLSX.utils.encode_cell({r:0,c:C2});
    if(!ws[addr])continue;
    ws[addr].s={font:{bold:true,color:{rgb:black}},fill:{fgColor:{rgb:gold.replace('FF','')}},alignment:{horizontal:'center'}};
  }
  return ws;
}
function generateReport(){
  const button=document.getElementById('generateReportButton');
  const status=document.getElementById('reportStatus');
  if(typeof XLSX==='undefined'){
    status.textContent='The report library could not load. Check your internet connection and try again.';
    return;
  }
  button.disabled=true;
  button.textContent='Generating…';
  status.textContent='Building your selected worksheets.';
  const wb=XLSX.utils.book_new();
  const sections=[
    ['contacts','All Contacts'],['shows','Shows'],['categories','Categories'],
    ['revenue','Revenue'],['retention','Retention'],['time','Booking Times'],
    ['groups','Group Sizes'],['seasons','Seasonality'],
    ['graph','Audience Overlap'],['outliers','Show Outliers'],
    ['greedy','Reach Optimisation'],['schedule','Schedule Plan'],
    ['gender','Gender Estimate'],['background','Cultural Background']
  ];
  let added=0;
  for(const[id,label] of sections){
    if(!isChecked(id)||!DATA[id])continue;
    const ws=XLSX.utils.aoa_to_sheet(DATA[id]);
    ws['!cols']=DATA[id][0].map(()=>({wch:28}));
    XLSX.utils.book_append_sheet(wb,ws,label);
    added++;
  }
  if(added===0){status.textContent='Select at least one report section.';button.disabled=false;button.textContent='Generate Excel Report';return;}
  try{
    const date=new Date().toISOString().slice(0,10);
    XLSX.writeFile(wb,'TheOneRoom_Report_'+date+'.xlsx');
    status.textContent=`Report ready with ${added} worksheet${added===1?'':'s'}.`;
  }catch(error){
    status.textContent='The report could not be generated. Please try again.';
  }finally{
    button.disabled=false;
    button.textContent='Generate Excel Report';
  }
}
""");
        html.append("</script>");
        html.append("</body></html>");
        sendHtml(ex, html.toString());
    }

    private static String jsStr(String s) {
        if (s == null) return "\"\"";
        StringBuilder out = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '\\': out.append("\\\\"); break;
                case '"': out.append("\\\""); break;
                case '\b': out.append("\\b"); break;
                case '\f': out.append("\\f"); break;
                case '\n': out.append("\\n"); break;
                case '\r': out.append("\\r"); break;
                case '\t': out.append("\\t"); break;
                case '<': out.append("\\u003C"); break;
                case '>': out.append("\\u003E"); break;
                case '&': out.append("\\u0026"); break;
                default:
                    if (ch < 0x20 || ch == '\u2028' || ch == '\u2029') out.append(String.format(Locale.ROOT,"\\u%04x",(int)ch));
                    else out.append(ch);
            }
        }
        return out.append('"').toString();
    }

    private static String sharedCss(){return "<style>body{margin:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;font-size:15px;line-height:1.5;background:radial-gradient(circle at top,#2b1b33 0,#0b0b10 40%,#050509 100%);color:#f5f5f5;}.wrapper{max-width:960px;margin:0 auto;padding:24px 16px 48px;}.hero{position:relative;padding:24px 20px;margin-bottom:18px;border-radius:18px;background:linear-gradient(135deg,rgba(255,204,128,0.06),rgba(255,255,255,0.02));border:1px solid rgba(255,255,255,0.06);box-shadow:0 18px 45px rgba(0,0,0,0.75);overflow:hidden;}.hero::before{content:'';position:absolute;right:-80px;top:-80px;width:220px;height:220px;background:radial-gradient(circle,#f2c46b 0,rgba(242,196,107,0) 55%);opacity:0.7;}.hero-title{font-size:24px;font-weight:600;margin:0 0 8px;letter-spacing:0.03em;text-transform:uppercase;position:relative;z-index:1;}.hero-sub{margin:0 0 4px;font-size:14px;color:rgba(255,255,255,0.8);position:relative;z-index:1;}.hero-meta{font-size:12px;color:rgba(255,255,255,0.45);position:relative;z-index:1;}.hero-pill{display:inline-block;padding:4px 10px;margin-bottom:8px;border-radius:999px;background:rgba(0,0,0,0.55);border:1px solid rgba(255,255,255,0.12);font-size:11px;letter-spacing:0.16em;text-transform:uppercase;color:#f8e7c3;position:relative;z-index:1;}.card{border-radius:16px;background:rgba(8,8,15,0.9);border:1px solid rgba(255,255,255,0.06);box-shadow:0 12px 30px rgba(0,0,0,0.7);padding:18px 20px 20px;margin-bottom:14px;}.card h2{margin:0 0 10px;font-size:17px;font-weight:600;color:#f8e7c3;}.tagline{font-size:13px;color:rgba(255,255,255,0.6);margin-bottom:10px;}table{width:100%;border-collapse:collapse;font-size:14px;}th,td{padding:9px 10px;border-bottom:1px solid rgba(255,255,255,0.05);text-align:left;}th{font-size:11px;text-transform:uppercase;letter-spacing:0.08em;color:rgba(248,231,195,0.7);background:rgba(255,255,255,0.02);}tbody tr:hover{background:rgba(255,255,255,0.025);}a{color:#f2c46b;text-decoration:none;}a:hover{color:#ffe3a5;text-decoration:underline;}.muted{color:rgba(255,255,255,0.45);font-size:12px;}code{background:rgba(255,255,255,0.07);border-radius:4px;padding:1px 5px;font-size:11px;}button{background:#f2c46b;color:#111;border:none;border-radius:999px;padding:10px 22px;font-size:13px;font-weight:600;cursor:pointer;}button:hover{background:#ffe3a5;}textarea{width:100%;min-height:180px;border-radius:10px;border:1px solid rgba(255,255,255,0.15);background:rgba(0,0,0,0.6);color:#f5f5f5;font-size:12px;padding:10px;resize:vertical;box-sizing:border-box;}pre{background:rgba(0,0,0,0.5);border:1px solid rgba(255,255,255,0.08);border-radius:10px;padding:12px;overflow-x:auto;font-size:11px;color:rgba(255,255,255,0.7);}select{background:rgba(20,20,32,0.95);color:#f0f0f0;border:1px solid rgba(255,255,255,0.15);border-radius:6px;padding:4px 8px;font-size:12px;}</style>";}
}
