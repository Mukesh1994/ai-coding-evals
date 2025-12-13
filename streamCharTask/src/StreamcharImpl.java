import java.util.HashMap;
import java.util.HashSet;
/*
This Uses a doubly linked list and hashmap
Time complexity = O(1)
Space Complexity = O(26) or if we count for linked list and map it is O(n)
 */
public class StreamcharImpl implements Streamchar{

    class Node {
        char val;
        Node prev;
        Node next;
        Node(char val) {
            this.val = val;
            this.prev = null;
            this.next = null;
        }
    }

    HashMap<Character,Node> characterNodeHashMap ;
    HashSet<Character> hashSet ;
    private Node head;
    private Node tail;
    public StreamcharImpl() {
        characterNodeHashMap = new HashMap<>();
        hashSet = new HashSet<>();
        head = new Node('/');
        tail = new Node('/'); // dummy nodes
        head.next = tail;
        tail.prev = head;
    }

    private void addAtLast(Node curNode) {
        Node lastNode  = tail.prev;
        lastNode.next = curNode;
        curNode.prev = lastNode;
        curNode.next = tail;
        tail.prev = curNode;
    }

    private void removeNode(Node curNode) {
        Node nextNode = curNode.next;
        Node prevNode = curNode.prev;
        prevNode.next = nextNode;
        nextNode.prev = prevNode;
    }

    @Override
    public void add(char ch) {
        if(!Util.isCharValid(ch)) return ;
        if(characterNodeHashMap.get(ch) == null) {
            Node newNode = new Node(ch);
            addAtLast(newNode);
            characterNodeHashMap.put(ch,newNode);
        } else {
            if(!hashSet.contains(ch)) {
                hashSet.add(ch);
                Node curNode = characterNodeHashMap.get(ch);
                removeNode(curNode);
            }
        }
    }

    @Override
    public char query() {
        if(head.next == tail ){
            return Util.DEFAULT_CHAR;
        } else {
            return head.next.val;
        }
    }
}
