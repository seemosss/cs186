package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import static edu.berkeley.cs186.database.concurrency.LockType.IX;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        // case 1
        if (LockType.substitutable(effectiveLockType, requestType)) {
//            lockContext.release(transaction);
//            lockContext.acquire(transaction, requestType);
            return;
        }

        //case 2
        if (requestType == LockType.S && explicitLockType == LockType.IX) {
            lockContext.promote(transaction, LockType.SIX);
            return;
        }

        //case 3
        if (explicitLockType.isIntent()) {
            lockContext.escalate(transaction);
            explicitLockType = lockContext.getExplicitLockType(transaction);
            if (explicitLockType == requestType || explicitLockType == LockType.X) return;
        }

        if (requestType == LockType.S) {
            grantAncestorLock(transaction, parentContext, LockType.IS);
        } else {
            grantAncestorLock(transaction, parentContext, LockType.IX);
        }
        if (explicitLockType == LockType.NL) lockContext.acquire(transaction, requestType);
        else lockContext.promote(transaction, requestType);
    }

    // TODO(proj4_part2) add any helper methods you want
    public static void grantAncestorLock(TransactionContext transactionContext, LockContext lockContext, LockType lockType) {
        if (transactionContext == null || lockContext == null) return;
        LockType thisType = lockContext.getExplicitLockType(transactionContext);
        if (thisType == lockType) return;
        if (thisType == LockType.IX) return;
        if (lockContext.parentContext() != null) {
            grantAncestorLock(transactionContext, lockContext.parentContext(), lockType);
        }
        if (thisType == LockType.NL) lockContext.acquire(transactionContext, lockType);
        if (thisType == LockType.IS) lockContext.promote(transactionContext, lockType);
    }
}
