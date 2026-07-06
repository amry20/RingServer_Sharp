/***************************************************************************
 * RBTree.cs
 *
 * Red-Black Tree implementation (generic version), implements balanced
 * binary search tree data structures.
 *
 * This file is part of the ringserver C# port.
 *
 * Original C code by Emin Marinian / Chad Trabant
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 ***************************************************************************/

using System;

namespace RingServer.Types;

/// <summary>
/// Node in the Red-Black tree
/// </summary>
public class RBNode
{
    public object? Key;       // String key (stream ID)
    public object? Data;      // Associated data
    public bool IsRed = true; // If false, the node is black
    public RBNode? Left;
    public RBNode? Right;
    public RBNode? Parent;

    public bool IsBlack => !IsRed;
}

/// <summary>
/// Red-Black Tree with string keys and object data.
/// Replaces the C version's RBTree with function pointers.
/// </summary>
public class RBTree
{
    private RBNode? _root;
    private RBNode? _nil;
    private readonly Func<object?, object?, int> _compareFunc;
    private readonly Action<object?>? _destroyKeyFunc;
    private readonly Action<object?>? _destroyDataFunc;

    public RBNode? Root => _root;
    public RBNode? Nil => _nil;

    /// <summary>
    /// Create a new Red-Black tree
    /// </summary>
    public RBTree(Func<object?, object?, int> compareFunc,
                  Action<object?>? destroyKeyFunc = null,
                  Action<object?>? destroyDataFunc = null)
    {
        _compareFunc = compareFunc;
        _destroyKeyFunc = destroyKeyFunc;
        _destroyDataFunc = destroyDataFunc;

        _nil = new RBNode { IsRed = false, Left = null, Right = null, Parent = null };
        _nil.Left = _nil;
        _nil.Right = _nil;
        _root = _nil;
    }

    /// <summary>
    /// Default tree for string comparison
    /// </summary>
    public static RBTree CreateStringTree()
    {
        return new RBTree(
            (a, b) => string.Compare(a?.ToString(), b?.ToString(), StringComparison.Ordinal),
            null,
            null
        );
    }

    /// <summary>
    /// Insert a key/data pair into the tree. Returns the inserted node.
    /// </summary>
    public RBNode Insert(object key, object data)
    {
        var node = new RBNode { Key = key, Data = data, IsRed = true };
        return InsertNode(node);
    }

    /// <summary>
    /// Internal node insertion
    /// </summary>
    private RBNode InsertNode(RBNode node)
    {
        var x = _root;
        var y = _nil;

        // Find where to insert
        while (x != _nil)
        {
            y = x;
            if (_compareFunc(node.Key, x.Key) < 0)
                x = x.Left!;
            else
                x = x.Right!;
        }

        node.Parent = y;
        if (y == _nil)
        {
            _root = node;
        }
        else if (_compareFunc(node.Key, y.Key) < 0)
        {
            y.Left = node;
        }
        else
        {
            y.Right = node;
        }

        node.Left = _nil;
        node.Right = _nil;
        node.IsRed = true;

        InsertFixup(node);
        return node;
    }

    /// <summary>
    /// Fix the red-black properties after insertion
    /// </summary>
    private void InsertFixup(RBNode z)
    {
        while (z.Parent?.IsRed == true)
        {
            if (z.Parent == z.Parent.Parent?.Left)
            {
                var y = z.Parent.Parent.Right;
                if (y?.IsRed == true)
                {
                    z.Parent.IsRed = false;
                    y.IsRed = false;
                    z.Parent.Parent.IsRed = true;
                    z = z.Parent.Parent;
                }
                else
                {
                    if (z == z.Parent.Right)
                    {
                        z = z.Parent;
                        LeftRotate(z);
                    }
                    if (z.Parent != null)
                    {
                        z.Parent.IsRed = false;
                        if (z.Parent.Parent != null)
                        {
                            z.Parent.Parent.IsRed = true;
                            RightRotate(z.Parent.Parent);
                        }
                    }
                }
            }
            else
            {
                var y = z.Parent?.Parent?.Left;
                if (y?.IsRed == true)
                {
                    if (z.Parent != null) z.Parent.IsRed = false;
                    if (y != null) y.IsRed = false;
                    if (z.Parent?.Parent != null) z.Parent.Parent.IsRed = true;
                    z = z.Parent.Parent;
                }
                else
                {
                    if (z == z.Parent?.Left)
                    {
                        z = z.Parent;
                        RightRotate(z);
                    }
                    if (z.Parent != null)
                    {
                        z.Parent.IsRed = false;
                        if (z.Parent.Parent != null)
                        {
                            z.Parent.Parent.IsRed = true;
                            LeftRotate(z.Parent.Parent);
                        }
                    }
                }
            }
        }
        if (_root != null) _root.IsRed = false;
    }

    /// <summary>
    /// Left rotation
    /// </summary>
    private void LeftRotate(RBNode x)
    {
        if (x.Right == _nil)
            throw new InvalidOperationException("LeftRotate with nil right child");

        var y = x.Right!;
        x.Right = y.Left!;
        if (y.Left != _nil)
            y.Left.Parent = x;
        y.Parent = x.Parent;
        if (x.Parent == _nil)
            _root = y;
        else if (x == x.Parent?.Left)
            x.Parent.Left = y;
        else
            x.Parent.Right = y;
        y.Left = x;
        x.Parent = y;
    }

    /// <summary>
    /// Right rotation
    /// </summary>
    private void RightRotate(RBNode x)
    {
        if (x.Left == _nil)
            throw new InvalidOperationException("RightRotate with nil left child");

        var y = x.Left!;
        x.Left = y.Right!;
        if (y.Right != _nil)
            y.Right.Parent = x;
        y.Parent = x.Parent;
        if (x.Parent == _nil)
            _root = y;
        else if (x == x.Parent?.Right)
            x.Parent.Right = y;
        else
            x.Parent.Left = y;
        y.Right = x;
        x.Parent = y;
    }

    /// <summary>
    /// Find a node by key
    /// </summary>
    public RBNode? Find(object key)
    {
        var x = _root;
        while (x != _nil)
        {
            int cmp = _compareFunc(key, x.Key);
            if (cmp == 0)
                return x;
            x = cmp < 0 ? x.Left! : x.Right!;
        }
        return null;
    }

    /// <summary>
    /// Find the minimum node (successor of nil)
    /// </summary>
    public RBNode? Minimum(RBNode? node)
    {
        if (node == _nil || node == null)
            return null;
        var x = node;
        while (x.Left != _nil)
            x = x.Left!;
        return x;
    }

    /// <summary>
    /// Find the maximum node
    /// </summary>
    public RBNode? Maximum(RBNode? node)
    {
        if (node == _nil || node == null)
            return null;
        var x = node;
        while (x.Right != _nil)
            x = x.Right!;
        return x;
    }

    /// <summary>
    /// Find the successor of a node
    /// </summary>
    public RBNode? Successor(RBNode? node)
    {
        if (node == _nil || node == null)
            return null;
        if (node.Right != _nil)
            return Minimum(node.Right);
        var y = node.Parent;
        while (y != _nil && node == y?.Right)
        {
            node = y;
            y = y.Parent;
        }
        return y;
    }

    /// <summary>
    /// Find the predecessor of a node
    /// </summary>
    public RBNode? Predecessor(RBNode? node)
    {
        if (node == _nil || node == null)
            return null;
        if (node.Left != _nil)
            return Maximum(node.Left);
        var y = node.Parent;
        while (y != _nil && node == y?.Left)
        {
            node = y;
            y = y.Parent;
        }
        return y;
    }

    /// <summary>
    /// Delete a node from the tree
    /// </summary>
    public void Delete(RBNode z)
    {
        var y = z;
        var x = _nil;
        bool yOriginalRed = y.IsRed;

        if (z.Left == _nil)
        {
            x = z.Right!;
            Transplant(z, z.Right!);
        }
        else if (z.Right == _nil)
        {
            x = z.Left!;
            Transplant(z, z.Left!);
        }
        else
        {
            y = Minimum(z.Right)!;
            yOriginalRed = y.IsRed;
            x = y.Right!;

            if (y.Parent == z)
            {
                x.Parent = y;
            }
            else
            {
                Transplant(y, y.Right!);
                y.Right = z.Right;
                y.Right.Parent = y;
            }

            Transplant(z, y);
            y.Left = z.Left;
            y.Left.Parent = y;
            y.IsRed = z.IsRed;
        }

        if (!yOriginalRed)
            DeleteFixup(x);
    }

    /// <summary>
    /// Replace one subtree with another
    /// </summary>
    private void Transplant(RBNode u, RBNode v)
    {
        if (u.Parent == _nil)
            _root = v;
        else if (u == u.Parent?.Left)
            u.Parent.Left = v;
        else
            u.Parent.Right = v;
        v.Parent = u.Parent;
    }

    /// <summary>
    /// Fix red-black properties after deletion
    /// </summary>
    private void DeleteFixup(RBNode x)
    {
        while (x != _root && x.IsBlack)
        {
            if (x == x.Parent?.Left)
            {
                var w = x.Parent.Right;
                if (w?.IsRed == true)
                {
                    w.IsRed = false;
                    x.Parent.IsRed = true;
                    LeftRotate(x.Parent);
                    w = x.Parent.Right;
                }
                if ((w?.Left?.IsBlack ?? true) && (w?.Right?.IsBlack ?? true))
                {
                    if (w != null) w.IsRed = true;
                    x = x.Parent;
                }
                else
                {
                    if (w?.Right?.IsBlack ?? true)
                    {
                        if (w?.Left != null) w.Left.IsRed = false;
                        if (w != null) w.IsRed = true;
                        if (w != null) RightRotate(w);
                        w = x.Parent.Right;
                    }
                    if (w != null) w.IsRed = x.Parent.IsRed;
                    x.Parent.IsRed = false;
                    if (w?.Right != null) w.Right.IsRed = false;
                    LeftRotate(x.Parent);
                    x = _root!;
                }
            }
            else
            {
                var w = x.Parent?.Left;
                if (w?.IsRed == true)
                {
                    w.IsRed = false;
                    x.Parent.IsRed = true;
                    RightRotate(x.Parent);
                    w = x.Parent?.Left;
                }
                if ((w?.Right?.IsBlack ?? true) && (w?.Left?.IsBlack ?? true))
                {
                    if (w != null) w.IsRed = true;
                    x = x.Parent;
                }
                else
                {
                    if (w?.Left?.IsBlack ?? true)
                    {
                        if (w?.Right != null) w.Right.IsRed = false;
                        if (w != null) w.IsRed = true;
                        if (w != null) LeftRotate(w);
                        w = x.Parent?.Left;
                    }
                    if (w != null) w.IsRed = x.Parent.IsRed;
                    x.Parent.IsRed = false;
                    if (w?.Left != null) w.Left.IsRed = false;
                    RightRotate(x.Parent);
                    x = _root!;
                }
            }
        }
        x.IsRed = false;
    }

    /// <summary>
    /// Walk the tree in-order and call a function for each node
    /// </summary>
    public void Traverse(Action<RBNode> nodeAction)
    {
        TraverseNode(_root, nodeAction);
    }

    private void TraverseNode(RBNode? node, Action<RBNode> nodeAction)
    {
        if (node == _nil || node == null)
            return;
        TraverseNode(node.Left, nodeAction);
        nodeAction(node);
        TraverseNode(node.Right, nodeAction);
    }

    /// <summary>
    /// Build a stack (linked list in in-order) from the tree
    /// </summary>
    public void BuildStack(Stack<object> stack)
    {
        Traverse(node => stack.Push(node.Data!));
    }

    /// <summary>
    /// Destroy the tree, optionally freeing keys and data
    /// </summary>
    public void Destroy()
    {
        Traverse(node =>
        {
            _destroyKeyFunc?.Invoke(node.Key);
            _destroyDataFunc?.Invoke(node.Data);
        });
        _root = _nil;
    }

    /// <summary>
    /// Find the data associated with a key
    /// </summary>
    public object? FindData(object key)
    {
        var node = Find(key);
        return node?.Data;
    }

    /// <summary>
    /// Remove (delete and return) a node from the tree by key
    /// </summary>
    public RBNode? Remove(object key)
    {
        var node = Find(key);
        if (node != null)
        {
            Delete(node);
        }
        return node;
    }
}
