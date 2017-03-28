package system;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * GraphDbTraversal Abstract Class
 * 
 * @author ksemer
 */
public abstract class GraphDbTraversal {
	// graph db instance
	protected GraphDatabaseService graphdb;

	public PathPrinter pathPrinter = new PathPrinter("name");

	/**
	 * RelationType for time index
	 * 
	 * @author ksemer
	 *
	 */
	public static enum RelTypes implements RelationshipType {
		EXISTS
	}

	public GraphDbTraversal(GraphDatabaseService graphdb) {
		this.graphdb = graphdb;
	}

	/**
	 * Reachability computation between src -> trg
	 * 
	 * @param src
	 * @param trg
	 * @param rel
	 * @return
	 */
	protected abstract boolean reachabilityBFS(Node src, Node trg, int t);

	/**
	 * Load graph db types
	 */
	protected abstract void loadTypes();

	/**
	 * Path computation for disjunctive query
	 * 
	 * @param src
	 * @param trg
	 * @param time_instance
	 * @return
	 */
	protected abstract String pathTraversalDisj(Node src, Node trg, int t);

	/**
	 * Path computation for conjunctive query
	 * 
	 * @param src
	 * @param trg
	 * @param rel
	 * @param times
	 * @return
	 */
	protected abstract String pathTraversalConj(Node src, Node trg, List<Integer> times);

	/**
	 * Path computation for at least query
	 * 
	 * @param src
	 * @param trg
	 * @param rel
	 * @param times
	 * @param k
	 * @return
	 */
	protected abstract String pathTraversalAtLeast(Node src, Node trg, List<Integer> times, int k);

	/**
	 * Traversal from src looking for disjunctive connections
	 * 
	 * @param src
	 * @param t
	 * @return
	 */
	protected abstract List<Node> reachabilityBFS(Node src, int t);

	/**
	 * Traversal from src looking for conjunctive connections
	 * 
	 * @param src
	 * @param times
	 * @return
	 */
	protected abstract List<Node> reachabilityBFSConj(Node src, List<Integer> times);

	/**
	 * Traversal from src looking for connections with at least k duration
	 * 
	 * @param src
	 * @param times
	 * @return
	 */
	protected abstract List<String> reachabilityBFSAtLeast(Node src, List<Integer> times, int k);

	/**
	 * Find all connected pairs that are connected at least 1 time
	 * 
	 * @param times
	 * @return
	 */
	public String disjunctiveReachability(Set<Integer> times) {
		Set<String> pairs = new HashSet<>();
		List<Node> comp;
		int u_id, v_id;
		String pair;

		for (int t : times) {
			Set<Node> nodes = new HashSet<>();
			Set<Node> found;

			if (Queries.TIME_INDEX_ENABLED) {

				Node time = graphdb.findNode(Queries.timeNodeLabel, Queries.timeNodeProperty, "" + t);

				for (Relationship rel : time.getRelationships()) {
					nodes.add(rel.getEndNode());
				}

				found = new HashSet<>(nodes.size());
			} else {
				// TODO
				found = new HashSet<>();
			}

			for (Node n : nodes) {
				if (found.contains(n))
					continue;

				comp = reachabilityBFS(n, t);

				for (int i = 0; i < comp.size(); i++) {
					u_id = Integer.parseInt("" + comp.get(i).getProperty("id"));

					for (int j = i + 1; j < comp.size(); j++) {
						v_id = Integer.parseInt("" + comp.get(j).getProperty("id"));

						if (u_id > v_id)
							pair = "" + v_id + "," + u_id;
						else
							pair = "" + u_id + "," + v_id;

						if (!pairs.contains(pair))
							pairs.add(pair);
					}
				}
				found.addAll(comp);
			}
		}

		if (pairs.isEmpty())
			return "no pairs exist";

		pair = "";
		for (String p : pairs) {
			pair += "(" + p + ")";
		}

		return pair;
	}

	/**
	 * Find all connected pairs that are connected in all times
	 * 
	 * @param times
	 * @return
	 */
	public String conjunctiveReachability(Set<Integer> times) {
		Set<String> pairs = new HashSet<>();
		Set<Node> nodes = new HashSet<>(), found;
		List<Node> comp;
		int u_id, v_id;
		String pair;

		for (int t : times) {
			Set<Node> tmp = new HashSet<>();

			if (Queries.TIME_INDEX_ENABLED) {

				Node time = graphdb.findNode(Queries.timeNodeLabel, Queries.timeNodeProperty, "" + t);

				for (Relationship rel : time.getRelationships()) {
					tmp.add(rel.getEndNode());
				}

				if (nodes.isEmpty()) {
					nodes.addAll(tmp);
				} else {
					nodes.retainAll(tmp);

					if (nodes.isEmpty())
						return "no pairs exist";
				}

			} else {
				// TODO
			}
		}

		found = new HashSet<>(nodes.size());
		List<Integer> times_ = new ArrayList<>(times);
		Collections.sort(times_);

		for (Node n : nodes) {
			if (found.contains(n))
				continue;

			comp = reachabilityBFSConj(n, times_);

			for (int i = 0; i < comp.size(); i++) {
				u_id = Integer.parseInt("" + comp.get(i).getProperty("id"));

				for (int j = i + 1; j < comp.size(); j++) {
					v_id = Integer.parseInt("" + comp.get(j).getProperty("id"));

					if (u_id > v_id)
						pair = "" + v_id + "," + u_id;
					else
						pair = "" + u_id + "," + v_id;

					if (!pairs.contains(pair))
						pairs.add(pair);
				}
			}
			found.addAll(comp);
		}

		pair = "";

		if (pairs.isEmpty())
			return "no pairs exist";

		for (String p : pairs) {
			pair += "(" + p + ")";
		}

		return pair;
	}

	/**
	 * Find all connected pairs that are connected at least k times
	 * 
	 * @param times
	 * @param k
	 * @return
	 */
	public String atLeastReachability(Set<Integer> times, int k) {

		Set<String> pairs = new HashSet<>();
		Map<Node, Counter> nodes = new HashMap<>();
		Set<String> found;
		Counter c;
		Node n;
		String pair;

		for (int t : times) {

			if (Queries.TIME_INDEX_ENABLED) {

				Node time = graphdb.findNode(Queries.timeNodeLabel, Queries.timeNodeProperty, "" + t);

				for (Relationship rel : time.getRelationships()) {
					n = rel.getEndNode();

					if ((c = nodes.get(n)) == null) {
						c = new Counter();
						nodes.put(n, c);
					} else
						c.increase();
				}
			} else {
				// TODO
			}
		}

		found = new HashSet<>(nodes.size());
		List<Integer> times_ = new ArrayList<>(times);
		Collections.sort(times_);

		for (Entry<Node, Counter> e : nodes.entrySet()) {
			n = e.getKey();

			if (e.getValue().getValue() < k )
				continue;

			found.addAll(reachabilityBFSAtLeast(n, times_, k));
		}

		if (pairs.isEmpty())
			return "no pairs exist";

		pair = "";
		for (String p : pairs) {
			pair += "(" + p + ")";
		}

		return pair;
	}

	/**
	 * Connection src -> trg in all times
	 * 
	 * @param src
	 * @param trg
	 * @param times
	 * @return
	 */
	public boolean conjunctiveReachability(Node src, Node trg, Set<Integer> times) {

		if (!checkLifespan(src, times).isEmpty() || !checkLifespan(trg, times).isEmpty())
			return false;

		for (int t : times) {
			if (!reachabilityBFS(src, trg, t)) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Connection src -> trg at least 1 time
	 * 
	 * @param src
	 * @param trg
	 * @param times
	 * @return
	 */
	public boolean disjunctiveReachability(Node src, Node trg, Set<Integer> times) {

		Set<Integer> t1, t2;

		t1 = new HashSet<>(times);
		t1.removeAll(checkLifespan(src, times));
		t2 = new HashSet<>(times);
		t2.removeAll(checkLifespan(trg, times));
		t1.retainAll(t2);

		for (int t : t1) {
			if (reachabilityBFS(src, trg, t)) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Connection src -> trg at least k times
	 * 
	 * @param src
	 * @param trg
	 * @param times
	 * @param k
	 * @return
	 */
	public boolean atLeastReachability(Node src, Node trg, Set<Integer> times, int k) {
		Set<Integer> t1, t2;

		t1 = new HashSet<>(times);
		t1.removeAll(checkLifespan(src, times));
		t2 = new HashSet<>(times);
		t2.removeAll(checkLifespan(trg, times));
		t1.retainAll(t2);

		if (t1.size() < k)
			return false;

		for (int t : t1) {
			if (reachabilityBFS(src, trg, t)) {
				if (--k == 0)
					return true;
			}
		}

		return true;
	}

	/**
	 * Path u -> v alive in all times
	 * 
	 * @param src
	 * @param trg
	 * @param times
	 * @return
	 */
	public String conjunctivePath(Node src, Node trg, Set<Integer> times) {
		String path = null;

		if (!checkLifespan(src, times).isEmpty() || !checkLifespan(trg, times).isEmpty())
			return "no path exists";

		List<Integer> times_ = new ArrayList<>(times);
		Collections.sort(times_);

		path = pathTraversalConj(src, trg, times_);

		if (path.isEmpty())
			return "no path exists";

		return path;
	}

	/**
	 * Path u -> v alive in at least 1 time
	 * 
	 * @param src
	 * @param trg
	 * @param times
	 * @return
	 */
	public String disjunctivePath(Node src, Node trg, Set<Integer> times) {

		Set<Integer> t1, t2;
		String path = null;

		t1 = new HashSet<>(times);
		t1.removeAll(checkLifespan(src, times));
		t2 = new HashSet<>(times);
		t2.removeAll(checkLifespan(trg, times));
		t1.retainAll(t2);

		for (int t : t1) {
			path = pathTraversalDisj(src, trg, t);

			if (!path.isEmpty())
				return path;
		}

		return "no path exists";
	}

	/**
	 * Path u -> alive in at least k times
	 * 
	 * @param src
	 * @param trg
	 * @param times
	 * @param k
	 * @return
	 */
	public String atLeastPath(Node src, Node trg, Set<Integer> times, int k) {

		Set<Integer> t1, t2;
		String path = "";

		t1 = new HashSet<>(times);
		t1.removeAll(checkLifespan(src, times));
		t2 = new HashSet<>(times);
		t2.removeAll(checkLifespan(trg, times));
		t1.retainAll(t2);

		if (t1.size() < k)
			return "no path exists";

		List<Integer> times_ = new ArrayList<>(t1);
		Collections.sort(times_);

		path = pathTraversalAtLeast(src, trg, times_, k);

		if (!path.isEmpty())
			return path;

		return "no path exists";
	}

	/**
	 * Return times where node n was alive
	 * 
	 * @param n
	 * @param times
	 * @return
	 */
	private Set<Integer> checkLifespan(Node n, Set<Integer> times) {
		Set<Integer> t = new HashSet<>(times);

		for (Relationship rel : n.getRelationships(RelTypes.EXISTS, Direction.INCOMING)) {
			t.remove(Integer.parseInt(rel.getStartNode().getProperty("timeInstance").toString()));
		}

		return t;
	}
}