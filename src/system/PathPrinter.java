package system;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Paths.PathDescriptor;

public class PathPrinter implements PathDescriptor<Path> {
	private final String nodePropertyKey;

	public PathPrinter(String nodePropertyKey) {
		this.nodePropertyKey = nodePropertyKey;
	}

	@Override
	public String nodeRepresentation(Path path, Node node) {
		return "(" + node.getProperty(nodePropertyKey, "") + ")";
	}

	@Override
	public String relationshipRepresentation(Path path, Node from, Relationship relationship) {
		String prefix = "--", suffix = "--";
		if (from.equals(relationship.getEndNode())) {
			prefix = "<--";
		} else {
			suffix = "-->";
		}
		return prefix + "[" + relationship.getType().name() + "]" + suffix;
	}
}
