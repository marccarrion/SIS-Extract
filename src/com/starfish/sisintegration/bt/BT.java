package com.starfish.sisintegration.bt;

public class BT {

     private BTNode root;
 
     public BT() {
         root = null;
     }

     public boolean isEmpty() {
         return root == null;
     }

     public void insert(BTNode data) {
         root = insert(root, data);
     }

     public BTNode getRoot() {
         return root; 
     }

     public BTNode insert(BTNode node, BTNode data) {
         if (node == null) {
             // node = new BTNode(data);
             node = data;
         }
         else
         {
             if (node.getRight() == null)
                 node.right = insert(node.right, data);
             else
                 node.left = insert(node.left, data);             
         }
         return node;
     }     

     public int countNodes() {
         return countNodes(root);
     }

     public int countNodes(BTNode r) {
         if (r == null)
             return 0;
         else
         {
             int l = 1;
             l += countNodes(r.getLeft());
             l += countNodes(r.getRight());
             return l;
         }
     }

     public void inorder() {
         inorder(root);
     }

     public void inorder(BTNode r) {
         if (r != null)
         {
             inorder(r.getLeft());
             System.out.print(r.toString() +" ");
             inorder(r.getRight());
         }
     }

     public void preorder() {
         preorder(root);
     }

     public void preorder(BTNode r) {
         if (r != null)
         {
             System.out.print(r.toString() +" ");
             preorder(r.getLeft());             
             preorder(r.getRight());
         }
     }

     public String preorderstr() {
         return preorderstr(root);
     }

     public String preorderstr(BTNode r) {
         if (r != null) {
             if (r.isOR() || r.isAND()) {
                String a = r.toString();
                String b = preorderstr(r.getLeft());
                String c = preorderstr(r.getRight());
                return "( " + a + " " + b + " " + c + " )";
             } else {
                return r.toString();
             }
         } else {
             return "";
         }
     }

     public void postorder() {
         postorder(root);
     }

     public void postorder(BTNode r) {
         if (r != null)
         {
             postorder(r.getLeft());             
             postorder(r.getRight());
             System.out.print(r.toString() +" ");
         }
     }     

     public String toString() {
         return preorderstr();
     }

     public void toCNF() {
        int iterations = 1;
        String before;
        String after;
        if ( root.isAND() || root.isOR() ) {
            while (true) {
                before = this.toString();
                // System.out.println("Before Iteration " + iterations); 
                // System.out.println(before); 
                BTNode transformedRoot = distribute(root, root.getLeft(), root.getRight(), "");
                root = transformedRoot;
                after = this.toString();
                // if (root.equals(transformedRoot)) {
                // System.out.println("After Iteration " + iterations);
                // System.out.println(after);
                iterations++;
                if (before.equals(after)) {
                    // System.out.println("Leaving the loop");
                    break;
                }
             }
             // root is now in CNF
         }
     }

     private BTNode distribute(BTNode node, BTNode left, BTNode right, String spaces) {
         String a = "void";
         String b = "void";
         if ( left != null) a = left.toString();
         if ( right != null) b = right.toString();
         // System.out.println(spaces + "Distributing " + node.getID() + " " + node.toString() + " " + a + " " + b);
         if (node.isOR()) {
            if (left != null && left.isAND()) {
                // System.out.println(spaces + "distribute right over left AND");
                BTNode and=new BTNode("A",null,null,null,null,null);
                BTNode or1=new BTNode("O",null,null,null,null,null);
                BTNode or2=new BTNode("O",null,null,null,null,null);
                or1.setLeft(new BTNode(left.getLeft()));
                or1.setRight(new BTNode(right));
                or2.setLeft(new BTNode(left.getRight()));
                or2.setRight(new BTNode(right)); 
                BT bt = new BT();
                bt.insert(and);
                bt.insert(or2);
                bt.insert(or1);
                node = and;
                left = or1;
                right = or2;
                // return and;
                /* Originally return new And( 
                                             new Or(((Operator)left).getLeft(), right),
                                             new Or(((Operator)left).getRight(), right));
                */
            } else if (right !=null && right.isAND()) {
                // System.out.println(spaces + "distribute left over right AND");
                BTNode and=new BTNode("A",null,null,null,null,null);
                BTNode or1=new BTNode("O",null,null,null,null,null);
                BTNode or2=new BTNode("O",null,null,null,null,null);
                or1.setLeft(new BTNode(right.getLeft()));
                or1.setRight(new BTNode(left));
                or2.setLeft(new BTNode(right.getRight()));
                or2.setRight(new BTNode(left)); 
                BT bt = new BT();
                bt.insert(and);
                bt.insert(or2);
                bt.insert(or1);
                node = and;
                left = or1;
                right = or2;
                // return and;
                /* Originally return new And(
                                             new Or(((Operator)right).getLeft(), left),
                                             new Or(((Operator)right).getRight(), left));
                */
            }
         }
         if(node.isAND() || node.isOR()) {
             // System.out.println(spaces+"Let's distribute the rest!!!");
             // System.out.println(spaces+"before " + left.toString() + " " + right.toString());
             left = distribute(node.getLeft(), left.getLeft(), left.getRight(), spaces + "  ");
             right = distribute(node.getRight(), right.getLeft(), right.getRight(), spaces + " ");
             // System.out.println(spaces+"after " + left.toString() + " " + right.toString());
             node.setLeft(left);
             node.setRight(right);
         }
         // default
         return node;
     }
 }
 
